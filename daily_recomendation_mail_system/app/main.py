#!/usr/bin/env python3
"""
=============================================================================
main.py - Daily Recommendation Mail System
=============================================================================
This script serves as the main entry point for the daily stock analysis and 
recommendation mail system. It automates the following workflow:

1. Fetches live and historical stock data from the National Stock Exchange (NSE).
2. Calculates technical indicators (RSI, MACD, Moving Averages).
3. Fetches related financial news and uses multi-model AI for sentiment analysis.
4. Evaluates the data to generate stock recommendations (Buy/Sell/Hold).
5. Sends a formatted HTML email with the recommendations to the user.
6. Logs the analysis results securely into Google BigQuery.
=============================================================================
"""
"""
Complete Free NSE Stock Analysis System - PRODUCTION READY
- jugaad-data PRIMARY for live prices (TICK → QUOTE → PREVIOUS_CLOSE)
- yfinance FALLBACK for historical data (kept for reliability)
- Multi-model AI with automatic fallback
- Robust AI JSON parsing
- News analysis + AI sentiment
- HTML email generation
- BigQuery integration
"""

import os
import json
import logging
import time
import re
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

import requests
import yfinance as yf
import pandas as pd
from jugaad_data.nse import NSELive
from ta.momentum import RSIIndicator
from ta.trend import SMAIndicator
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ===========================
# FREE AI MODELS (PRIORITY ORDER)
# ===========================

FREE_MODELS = [
    "arcee-ai/trinity-large-preview:free",
    "deepseek/deepseek-r1-0528:free",
    "google/gemma-3-4b-it:free",
    "openai/gpt-oss-120b:free"
]


# ===========================
# ENUMS & DATA CLASSES
# ===========================

class StockAction(Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class NewsSentiment(Enum):
    POSITIVE = "POSITIVE"
    NEGATIVE = "NEGATIVE"
    NEUTRAL = "NEUTRAL"


@dataclass
class StockRecommendation:
    symbol: str
    company_name: str
    current_price: float
    action: StockAction
    target_price: float
    confidence: str
    reasoning: str
    entry_range: tuple
    stop_loss: float
    upside_potential: float
    technical_summary: str
    news_summary: str
    risk_factors: List[str]
    time_horizon: str
    data_source: str


# ===========================
# NSE DATA FETCHER
# ===========================

class NSEDataFetcher:
    """Fetch data with jugaad PRIMARY for live price, yfinance for historical only"""
    
    def __init__(self):
        try:
            self.nse = NSELive()
            logger.info("NSELive initialized successfully")
        except Exception as e:
            logger.error(f"NSELive initialization failed: {e}")
            self.nse = None
    
    def get_stock_data(self, symbol: str) -> Dict[str, Any]:
        """Get current price - jugaad ONLY (TICK → QUOTE → PREVIOUS_CLOSE)"""
        
        # PRIORITY 1: Try tick data (best during market hours)
        try:
            if self.nse:
                tick = self.nse.tick_data(symbol)
                price = float(tick["grapthData"][-1][1])
                logger.info(f"{symbol}: Price from TICK = ₹{price}")
                return {
                    'symbol': symbol,
                    'current_price': price,
                    'previous_close': price * 0.99,
                    'source': 'TICK'
                }
        except Exception as e:
            logger.debug(f"Tick data failed for {symbol}: {e}")
        
        # PRIORITY 2: Try live quote
        try:
            if self.nse:
                q = self.nse.stock_quote(symbol)
                price = float(q["priceInfo"]["lastPrice"])
                logger.info(f"{symbol}: Price from QUOTE = ₹{price}")
                return {
                    'symbol': symbol,
                    'current_price': price,
                    'previous_close': float(q["priceInfo"]["previousClose"]),
                    'open': float(q["priceInfo"]["open"]),
                    'high': float(q["priceInfo"]["intraDayHighLow"]["max"]),
                    'low': float(q["priceInfo"]["intraDayHighLow"]["min"]),
                    'volume': 0,
                    'change': float(q["priceInfo"]["change"]),
                    'pct_change': float(q["priceInfo"]["pChange"]),
                    '52w_high': float(q["priceInfo"]["weekHighLow"]["max"]),
                    '52w_low': float(q["priceInfo"]["weekHighLow"]["min"]),
                    'source': 'QUOTE'
                }
        except Exception as e:
            logger.debug(f"Quote failed for {symbol}: {e}")
        
        # PRIORITY 3: Try previous close
        try:
            if self.nse:
                q = self.nse.stock_quote(symbol)
                price = float(q["priceInfo"]["previousClose"])
                logger.info(f"{symbol}: Price from PREVIOUS_CLOSE = ₹{price}")
                return {
                    'symbol': symbol,
                    'current_price': price,
                    'previous_close': price,
                    'source': 'PREVIOUS_CLOSE'
                }
        except Exception as e:
            logger.warning(f"All jugaad endpoints failed for {symbol}: {e}")
            raise ValueError(f"Cannot fetch price for {symbol}")
    
    def get_historical_data(self, symbol: str, days: int = 50) -> pd.DataFrame:
        """Get historical data - jugaad first, then yfinance (KEPT FOR RELIABILITY)"""

        # -------------------------------
        # 1️⃣ Try jugaad tick history
        # -------------------------------
        try:
            if self.nse:
                tick = self.nse.tick_data(symbol)

                if tick and "grapthData" in tick and tick["grapthData"]:
                    df = pd.DataFrame(tick["grapthData"], columns=["time", "Close"])
                    df["time"] = pd.to_datetime(df["time"], unit="ms")
                    df.set_index("time", inplace=True)

                    if len(df) >= 20:
                        logger.info(f"{symbol}: Historical from jugaad ({len(df)} rows)")
                        return df

        except Exception as e:
            logger.debug(f"Jugaad tick history failed: {e}")

        # -------------------------------
        # 2️⃣ yfinance fallback (KEPT - uses start/end dates)
        # -------------------------------
        try:
            yf_symbol = symbol if symbol.endswith(".NS") else f"{symbol}.NS"
            
            # Use start/end dates instead of period (more reliable)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days + 30)
            
            # Create session with headers
            session = requests.Session()
            session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            
            ticker = yf.Ticker(yf_symbol, session=session)
            
            hist = ticker.history(
                start=start_date,
                end=end_date,
                interval="1d",
                auto_adjust=True,
                actions=False,
                prepost=False
            )

            if hist is not None and not hist.empty:
                hist = hist.tail(days)
                logger.info(f"{symbol}: Historical from yfinance ({len(hist)} rows)")
                return hist

            # Shorter period fallback
            logger.debug(f"{symbol}: yfinance empty, trying 10d fallback")
            start_date = end_date - timedelta(days=15)
            hist = ticker.history(start=start_date, end=end_date, interval="1d", auto_adjust=True, actions=False)

            if hist is not None and not hist.empty:
                logger.info(f"{symbol}: yfinance 10d fallback OK ({len(hist)} rows)")
                return hist

        except Exception as e:
            logger.debug(f"yfinance failed: {e}")

        # -------------------------------
        # 3️⃣ Final fallback - price-based analysis
        # -------------------------------
        logger.info(f"{symbol}: No historical data - will use price-based analysis")
        return pd.DataFrame()


# ===========================
# TECHNICAL ANALYZER
# ===========================

class TechnicalAnalyzer:
    """Calculate technical indicators"""
    
    @staticmethod
    def calculate_rsi(data: pd.DataFrame, period: int = 14) -> float:
        """Calculate RSI"""
        if len(data) < period + 1:
            return 50.0
        
        try:
            rsi_indicator = RSIIndicator(data['Close'], window=period)
            rsi = rsi_indicator.rsi()
            return float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else 50.0
        except:
            return 50.0
    
    @staticmethod
    def calculate_moving_averages(data: pd.DataFrame) -> Dict[str, float]:
        """Calculate moving averages"""
        mas = {}
        for period in [20, 50, 200]:
            if len(data) >= period:
                ma = data['Close'].rolling(window=period).mean()
                mas[f'MA{period}'] = float(ma.iloc[-1]) if not pd.isna(ma.iloc[-1]) else 0.0
            else:
                mas[f'MA{period}'] = 0.0
        return mas
    
    @staticmethod
    def calculate_macd(data: pd.DataFrame) -> str:
        """Calculate MACD signal"""
        if len(data) < 26:
            return "NEUTRAL"
        
        try:
            exp1 = data['Close'].ewm(span=12, adjust=False).mean()
            exp2 = data['Close'].ewm(span=26, adjust=False).mean()
            macd = exp1 - exp2
            signal = macd.ewm(span=9, adjust=False).mean()
            
            if macd.iloc[-1] > signal.iloc[-1]:
                return "BULLISH"
            elif macd.iloc[-1] < signal.iloc[-1]:
                return "BEARISH"
        except:
            pass
        
        return "NEUTRAL"
    
    @staticmethod
    def get_trend(data: pd.DataFrame, days: int) -> float:
        """Calculate price trend over period"""
        if len(data) < days:
            return 0.0
        
        try:
            old_price = data['Close'].iloc[-days]
            new_price = data['Close'].iloc[-1]
            return float(((new_price - old_price) / old_price) * 100)
        except:
            return 0.0


# ===========================
# NEWS ANALYZER WITH MULTI-MODEL AI
# ===========================

class NewsAnalyzer:
    """Analyze news sentiment with multi-model AI fallback"""
    
    def __init__(self, gnews_api_key: str, newsapi_key: str, openrouter_api_key: str):
        self.gnews_api_key = gnews_api_key
        self.newsapi_key = newsapi_key
        self.openrouter_api_key = openrouter_api_key
    
    def fetch_news(self, company_name: str, symbol: str) -> List[Dict[str, Any]]:
        """Fetch news from multiple sources"""
        all_news = []
        
        # Try GNews
        if self.gnews_api_key:
            try:
                all_news.extend(self._fetch_gnews(company_name))
            except Exception as e:
                logger.warning(f"GNews failed: {e}")
        
        # Try NewsAPI
        if self.newsapi_key and len(all_news) < 5:
            try:
                all_news.extend(self._fetch_newsapi(company_name))
            except Exception as e:
                logger.warning(f"NewsAPI failed: {e}")
        
        return all_news[:10]
    
    def _fetch_gnews(self, query: str) -> List[Dict[str, Any]]:
        """Fetch from GNews"""
        url = "https://gnews.io/api/v4/search"
        params = {
            'q': query,
            'lang': 'en',
            'country': 'in',
            'max': 5,
            'apikey': self.gnews_api_key
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        articles = []
        for article in data.get('articles', []):
            articles.append({
                'title': article.get('title', ''),
                'description': article.get('description', ''),
                'source': article.get('source', {}).get('name', 'GNews')
            })
        
        return articles
    
    def _fetch_newsapi(self, query: str) -> List[Dict[str, Any]]:
        """Fetch from NewsAPI"""
        url = "https://newsapi.org/v2/everything"
        from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        params = {
            'q': query,
            'from': from_date,
            'language': 'en',
            'sortBy': 'relevancy',
            'pageSize': 5,
            'apiKey': self.newsapi_key
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        articles = []
        for article in data.get('articles', []):
            articles.append({
                'title': article.get('title', ''),
                'description': article.get('description', ''),
                'source': article.get('source', {}).get('name', 'NewsAPI')
            })
        
        return articles
    
    def analyze_sentiment(self, news_articles: List[Dict[str, Any]], symbol: str) -> Dict[str, Any]:
        """Analyze sentiment with MULTI-MODEL AI fallback"""
        if not news_articles:
            return {
                'sentiment': NewsSentiment.NEUTRAL,
                'confidence': 'LOW',
                'summary': 'No recent news available.',
                'key_points': []
            }
        
        news_text = "\n\n".join([
            f"Title: {a['title']}\nDescription: {a.get('description', '')}"
            for a in news_articles[:5]
        ])
        
        # Try AI analysis with multi-model fallback
        if self.openrouter_api_key:
            ai_result = self._call_ai_with_fallback(news_text, symbol)
            if ai_result:
                return ai_result
        
        # Final fallback: keyword analysis
        return self._keyword_sentiment(news_text)
    
    def _call_ai_with_fallback(self, news_text: str, symbol: str) -> Optional[Dict[str, Any]]:
        """Call AI with automatic model switching on failure"""
        
        prompt = f"""Analyze news sentiment for {symbol} stock.

News articles:
{news_text}

Respond with ONLY valid JSON (no markdown, no backticks, no extra text):
{{
    "sentiment": "POSITIVE" or "NEGATIVE" or "NEUTRAL",
    "confidence": "HIGH" or "MEDIUM" or "LOW",
    "summary": "brief 2-3 sentence summary",
    "key_points": ["point 1", "point 2", "point 3"]
}}"""

        # Shuffle models for random selection
        models_to_try = FREE_MODELS.copy()
        random.shuffle(models_to_try)
        
        # Try each model with 2 attempts
        for model in models_to_try:
            for attempt in range(2):
                try:
                    if attempt > 0:
                        time.sleep(1)
                    
                    logger.info(f"Trying AI model: {model} (attempt {attempt + 1})")
                    
                    response = requests.post(
                        "https://openrouter.ai/api/v1/chat/completions",
                        headers={
                            "Authorization": f"Bearer {self.openrouter_api_key}",
                            "Content-Type": "application/json",
                            "HTTP-Referer": "https://github.com/your-username/your-repo",
                        },
                        json={
                            "model": model,
                            "messages": [{"role": "user", "content": prompt}],
                            "temperature": 0.3
                        },
                        timeout=30
                    )
                    
                    # Rate limit - try next model
                    if response.status_code == 429:
                        logger.warning(f"⚠️ Rate limit on {model}, switching model")
                        break
                    
                    # Success
                    if response.status_code == 200:
                        ai_response = response.json()
                        content = ai_response['choices'][0]['message']['content']
                        
                        # ROBUST JSON extraction
                        result = self._extract_json(content)
                        if result:
                            logger.info(f"✅ AI success with {model}")
                            return result
                    
                    # Other error - log and try again
                    logger.debug(f"{model} returned {response.status_code}")
                
                except requests.Timeout:
                    logger.debug(f"{model} timeout")
                except Exception as e:
                    logger.debug(f"{model} error: {e}")
        
        logger.warning("All AI models failed, using keyword fallback")
        return None
    
    def _extract_json(self, content: str) -> Optional[Dict[str, Any]]:
        """Robust JSON extraction from AI response"""
        
        # Method 1: Direct JSON parse
        try:
            result = json.loads(content.strip())
            result['sentiment'] = NewsSentiment[result['sentiment']]
            return result
        except:
            pass
        
        # Method 2: Extract from markdown code block
        json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', content, re.DOTALL)
        if json_match:
            try:
                result = json.loads(json_match.group(1))
                result['sentiment'] = NewsSentiment[result['sentiment']]
                return result
            except:
                pass
        
        # Method 3: Find first JSON object
        json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', content, re.DOTALL)
        if json_match:
            try:
                result = json.loads(json_match.group())
                result['sentiment'] = NewsSentiment[result['sentiment']]
                return result
            except:
                pass
        
        # Method 4: Clean and parse
        try:
            cleaned = content.replace('```json', '').replace('```', '').strip()
            start = cleaned.find('{')
            end = cleaned.rfind('}')
            if start != -1 and end != -1:
                json_str = cleaned[start:end+1]
                result = json.loads(json_str)
                result['sentiment'] = NewsSentiment[result['sentiment']]
                return result
        except:
            pass
        
        return None
    
    def _keyword_sentiment(self, text: str) -> Dict[str, Any]:
        """Keyword-based sentiment fallback"""
        text_lower = text.lower()
        
        positive = ['profit', 'growth', 'gain', 'rise', 'surge', 'rally', 
                   'bullish', 'upgrade', 'beat', 'strong', 'positive', 'expansion']
        negative = ['loss', 'fall', 'drop', 'decline', 'crash', 'bearish',
                   'downgrade', 'miss', 'weak', 'negative', 'concern']
        
        pos_count = sum(text_lower.count(w) for w in positive)
        neg_count = sum(text_lower.count(w) for w in negative)
        
        if pos_count > neg_count * 1.5:
            sentiment = NewsSentiment.POSITIVE
            confidence = 'MEDIUM'
        elif neg_count > pos_count * 1.5:
            sentiment = NewsSentiment.NEGATIVE
            confidence = 'MEDIUM'
        else:
            sentiment = NewsSentiment.NEUTRAL
            confidence = 'LOW'
        
        return {
            'sentiment': sentiment,
            'confidence': confidence,
            'summary': f'Keyword analysis: {pos_count} positive, {neg_count} negative mentions.',
            'key_points': ['Based on keyword frequency in news articles']
        }


# ===========================
# RECOMMENDATION ENGINE
# ===========================

class RecommendationEngine:
    """Generate recommendations"""
    
    def generate_recommendation(
        self,
        stock_data: Dict[str, Any],
        technical_data: Dict[str, Any],
        news_sentiment: Dict[str, Any],
        company_name: str
    ) -> StockRecommendation:
        """Generate recommendation"""
        
        symbol = stock_data['symbol']
        current_price = stock_data['current_price']
        data_source = stock_data.get('source', 'UNKNOWN')
        
        # Calculate scores
        tech_score = self._calculate_technical_score(technical_data)
        sent_score = self._calculate_sentiment_score(news_sentiment)
        
        total_score = (tech_score * 0.6) + (sent_score * 0.4)
        
        # Determine action
        if total_score >= 65:
            action = StockAction.BUY
            target_mult = 1.05 + (total_score - 65) / 500
            confidence = 'HIGH' if total_score >= 75 else 'MEDIUM'
        elif total_score <= 35:
            action = StockAction.SELL
            target_mult = 0.95 - (35 - total_score) / 500
            confidence = 'HIGH' if total_score <= 25 else 'MEDIUM'
        else:
            action = StockAction.HOLD
            target_mult = 1.02
            confidence = 'LOW'
        
        target_price = current_price * target_mult
        
        # Entry/stop
        if action == StockAction.BUY:
            entry_low = current_price * 0.99
            entry_high = current_price * 1.01
            stop_loss = current_price * 0.97
        elif action == StockAction.SELL:
            entry_low = current_price * 0.99
            entry_high = current_price * 1.01
            stop_loss = current_price * 1.03
        else:
            entry_low = current_price
            entry_high = current_price
            stop_loss = current_price * 0.98
        
        upside = ((target_price - current_price) / current_price) * 100
        
        # Summaries
        rsi = technical_data.get('rsi', 50)
        macd = technical_data.get('macd', 'NEUTRAL')
        trend_7d = technical_data.get('trend_7d', 0)
        
        tech_summary = f"RSI: {rsi:.1f} ({'Overbought' if rsi > 70 else 'Oversold' if rsi < 30 else 'Neutral'}), "
        tech_summary += f"MACD: {macd}, 7-day trend: {trend_7d:+.2f}%"
        
        news_summary = news_sentiment.get('summary', 'No news.')
        
        # Risks
        risk_factors = []
        if rsi > 70:
            risk_factors.append("Stock is overbought")
        if technical_data.get('trend_30d', 0) < -10:
            risk_factors.append("Negative 30-day trend")
        if news_sentiment['sentiment'] == NewsSentiment.NEGATIVE:
            risk_factors.append("Negative news sentiment")
        if not risk_factors:
            risk_factors.append("Standard market risks apply")
        
        reasoning = f"Technical Score: {tech_score:.1f}/100, Sentiment Score: {sent_score:.1f}/100. "
        if action == StockAction.BUY:
            reasoning += "Positive technical setup with favorable sentiment."
        elif action == StockAction.SELL:
            reasoning += "Weak technicals with negative sentiment."
        else:
            reasoning += "Mixed signals suggest caution."
        
        return StockRecommendation(
            symbol=symbol,
            company_name=company_name,
            current_price=current_price,
            action=action,
            target_price=target_price,
            confidence=confidence,
            reasoning=reasoning,
            entry_range=(entry_low, entry_high),
            stop_loss=stop_loss,
            upside_potential=upside,
            technical_summary=tech_summary,
            news_summary=news_summary,
            risk_factors=risk_factors,
            time_horizon="1-3 days" if abs(upside) > 5 else "1-2 weeks",
            data_source=data_source
        )
    
    def _calculate_technical_score(self, tech: Dict[str, Any]) -> float:
        """Technical score (0-100)"""
        score = 50
        
        rsi = tech.get('rsi', 50)
        if 40 <= rsi <= 60:
            score += 10
        elif 30 <= rsi < 40:
            score += 15
        elif rsi < 30:
            score += 20
        elif rsi > 70:
            score -= 15
        
        macd = tech.get('macd', 'NEUTRAL')
        if macd == 'BULLISH':
            score += 15
        elif macd == 'BEARISH':
            score -= 15
        
        trend_7d = tech.get('trend_7d', 0)
        score += min(20, max(-20, trend_7d * 2))
        
        trend_30d = tech.get('trend_30d', 0)
        score += min(10, max(-10, trend_30d))
        
        return max(0, min(100, score))
    
    def _calculate_sentiment_score(self, sent: Dict[str, Any]) -> float:
        """Sentiment score (0-100)"""
        sentiment = sent['sentiment']
        confidence = sent['confidence']
        
        if sentiment == NewsSentiment.POSITIVE:
            base = 70
        elif sentiment == NewsSentiment.NEGATIVE:
            base = 30
        else:
            base = 50
        
        mult = {'HIGH': 1.2, 'MEDIUM': 1.0, 'LOW': 0.8}.get(confidence, 1.0)
        
        return max(0, min(100, base * mult))


# ===========================
# BIGQUERY SENDER
# ===========================

class BigQuerySender:
    """Send recommendations to BigQuery"""

    def __init__(self, project_id: str, dataset_id: str, table_id: str):
        self.client = bigquery.Client(project=project_id) if project_id else None
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}" if project_id else None

    def send_recommendation_v2(self, recs: List[StockRecommendation]) -> bool:
        if not self.client or not self.table_ref:
            logger.info("BigQuery not configured, skipping")
            return True
            
        if not recs:
            return True

        try:
            rows = []

            for rec in recs:
                if not rec.entry_range or len(rec.entry_range) != 2:
                    raise ValueError(f"Invalid entry_range for {rec.symbol}")

                rows.append({
                    "symbol": rec.symbol,
                    "company_name": rec.company_name,
                    "current_price": rec.current_price,
                    "action": rec.action.value,
                    "target_price": rec.target_price,
                    "confidence": rec.confidence,
                    "reasoning": rec.reasoning,
                    "entry_low": rec.entry_range[0],
                    "entry_high": rec.entry_range[1],
                    "stop_loss": rec.stop_loss,
                    "upside_potential": rec.upside_potential,
                    "technical_summary": rec.technical_summary,
                    "news_summary": rec.news_summary,
                    "risk_factors": json.dumps(rec.risk_factors),
                    "time_horizon": rec.time_horizon,
                    "data_source": rec.data_source,
                    "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                })

            errors = self.client.insert_rows_json(self.table_ref, rows)

            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
                return False

            logger.info(f"✅ Inserted {len(rows)} recommendations into BigQuery")
            return True

        except Exception as e:
            logger.error(f"BigQuery insert failed: {e}")
            return False


# ===========================
# EMAIL SENDER
# ===========================

class EmailSender:
    """Send HTML emails"""
    
    def __init__(self, email_api_url: str):
        self.email_api_url = email_api_url
    
    def send_recommendation(self, to_email: str, recs: List[StockRecommendation]) -> bool:
        """Send email"""
        
        html = self._format_html(recs)
        
        # Get IST time
        ist_timezone = ZoneInfo("Asia/Kolkata")
        now_ist = datetime.now(ist_timezone)
        formatted_time = now_ist.strftime('%d %b %Y %H:%M %p %Z')
        
        payload = {
            "to_mail": to_email,
            "subject": f"📈 Stock Recommendations - {formatted_time}",
            "content_type": "html",
            "content": html
        }
        
        try:
            response = requests.post(
                self.email_api_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            if response.status_code >= 400:
                logger.error(f"Email API error: {response.status_code}")
                return False
            
            logger.info(f"✅ Email sent to {to_email}")
            return True
        except Exception as e:
            logger.error(f"Email failed: {e}")
            return False
    
    def _format_html(self, recs: List[StockRecommendation]) -> str:
        """Generate HTML"""
        
        html = f"""<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                  color: white; padding: 20px; text-align: center; border-radius: 10px 10px 0 0; }}
        .recommendation {{ background: #f8f9fa; margin: 20px 0; padding: 20px;
                         border-radius: 10px; border-left: 5px solid #667eea; }}
        .buy {{ border-left-color: #28a745; }}
        .sell {{ border-left-color: #dc3545; }}
        .hold {{ border-left-color: #ffc107; }}
        .metric {{ display: inline-block; margin: 10px 15px 10px 0; }}
        .label {{ font-weight: bold; color: #666; }}
        .value {{ color: #333; }}
        .section {{ margin: 15px 0; }}
        .risk {{ background: #fff3cd; padding: 10px; border-radius: 5px; margin: 10px 0; }}
        .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
        .badge {{ background: #e3f2fd; padding: 5px 10px; border-radius: 5px;
                font-size: 11px; color: #1976d2; display: inline-block; margin-top: 10px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🎯 Daily Stock Trading Recommendations</h1>
        <p>AI-Powered Analysis | {datetime.now().strftime('%d %B %Y')}</p>
    </div>
"""
        
        for rec in recs:
            action_class = rec.action.value.lower()
            emoji = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}[rec.action.value]
            
            html += f"""
    <div class="recommendation {action_class}">
        <h2>{rec.symbol} - {rec.company_name}</h2>
        <span class="badge">Data Source: {rec.data_source}</span>
        
        <div class="section">
            <div class="metric">
                <span class="label">📋 RECOMMENDATION:</span>
                <span class="value" style="font-size: 18px; font-weight: bold;">
                    {emoji} {rec.action.value}
                </span>
            </div>
            <div class="metric">
                <span class="label">🎯 TARGET:</span>
                <span class="value">₹{rec.target_price:.2f}</span>
            </div>
            <div class="metric">
                <span class="label">⏰ HORIZON:</span>
                <span class="value">{rec.time_horizon}</span>
            </div>
            <div class="metric">
                <span class="label">📊 CONFIDENCE:</span>
                <span class="value">{rec.confidence}</span>
            </div>
        </div>
        
        <div class="section">
            <h3>📈 Entry Strategy</h3>
            <div class="metric">
                <span class="label">Current Price:</span>
                <span class="value">₹{rec.current_price:.2f}</span>
            </div>
            <div class="metric">
                <span class="label">Entry Range:</span>
                <span class="value">₹{rec.entry_range[0]:.2f} - ₹{rec.entry_range[1]:.2f}</span>
            </div>
            <div class="metric">
                <span class="label">Stop Loss:</span>
                <span class="value">₹{rec.stop_loss:.2f}</span>
            </div>
            <div class="metric">
                <span class="label">Upside:</span>
                <span class="value">{rec.upside_potential:+.2f}%</span>
            </div>
        </div>
        
        <div class="section">
            <h3>💡 Rationale</h3>
            <p>{rec.reasoning}</p>
            <p><strong>Technical:</strong> {rec.technical_summary}</p>
        </div>
        
        <div class="section">
            <h3>📰 News Summary</h3>
            <p>{rec.news_summary}</p>
        </div>
        
        <div class="risk">
            <h3>⚠️ Key Risks</h3>
            <ul>
"""
            for risk in rec.risk_factors:
                html += f"                <li>{risk}</li>\n"
            
            html += """            </ul>
        </div>
    </div>
"""
        
        html += """
    <div class="footer">
        <p>⚠️ <strong>Disclaimer:</strong> AI-generated analysis for educational purposes only.
        Not financial advice. Conduct your own research before investing.</p>
        <p>Data: NSE (jugaad-data + yfinance), Technical Indicators, Multi-Model AI</p>
    </div>
</body>
</html>
"""
        
        return html


# ===========================
# MAIN SYSTEM
# ===========================

class StockAnalysisSystem:
    """Main system"""
    
    def __init__(self, gnews_key: str, newsapi_key: str, openrouter_key: str,
                 email_api_url: str, recipient_email: str):
        self.data_fetcher = NSEDataFetcher()
        self.news_analyzer = NewsAnalyzer(gnews_key, newsapi_key, openrouter_key)
        self.rec_engine = RecommendationEngine()
        self.email_sender = EmailSender(email_api_url)
        self.recipient_email = recipient_email
        self.bigquery_sender = BigQuerySender(
            project_id=os.getenv('BQ_PROJECT_ID', ''),
            dataset_id=os.getenv('BQ_DATASET_ID', ''),
            table_id=os.getenv('BQ_TABLE_ID', '')
        )
    
    def analyze_stock(self, symbol: str, company_name: str) -> Optional[StockRecommendation]:
        """Analyze one stock"""
        try:
            logger.info(f"Analyzing {symbol} - {company_name}")
            
            # Get live price from jugaad
            stock_data = self.data_fetcher.get_stock_data(symbol)
            
            # Get historical data (jugaad → yfinance fallback)
            hist_data = self.data_fetcher.get_historical_data(symbol, days=200)
            
            # Calculate indicators
            if not hist_data.empty and len(hist_data) >= 20:
                tech_data = {
                    'rsi': TechnicalAnalyzer.calculate_rsi(hist_data),
                    'mas': TechnicalAnalyzer.calculate_moving_averages(hist_data),
                    'macd': TechnicalAnalyzer.calculate_macd(hist_data),
                    'trend_7d': TechnicalAnalyzer.get_trend(hist_data, 7),
                    'trend_30d': TechnicalAnalyzer.get_trend(hist_data, 30),
                }
                logger.info(f"{symbol}: Using real technical indicators")
            else:
                # Price-based heuristics
                current = stock_data['current_price']
                prev = stock_data.get('previous_close', current)
                change_pct = ((current - prev) / prev) * 100 if prev else 0
                
                # Improved heuristics
                if change_pct > 3:
                    rsi, macd, trend_7d, trend_30d = 30, "BULLISH", 5.0, 3.0
                elif change_pct > 1.5:
                    rsi, macd, trend_7d, trend_30d = 40, "BULLISH", 3.0, 1.5
                elif change_pct < -3:
                    rsi, macd, trend_7d, trend_30d = 70, "BEARISH", -5.0, -3.0
                elif change_pct < -1.5:
                    rsi, macd, trend_7d, trend_30d = 60, "BEARISH", -3.0, -1.5
                else:
                    rsi = 50 + (change_pct * 5)
                    macd = "BULLISH" if change_pct > 0 else "BEARISH" if change_pct < 0 else "NEUTRAL"
                    trend_7d = change_pct * 1.5
                    trend_30d = change_pct * 0.8
                
                tech_data = {
                    'rsi': max(10, min(90, rsi)),
                    'mas': {'MA20': current, 'MA50': current, 'MA200': current},
                    'macd': macd,
                    'trend_7d': trend_7d,
                    'trend_30d': trend_30d,
                }
                logger.info(f"{symbol}: Price-based analysis (Δ{change_pct:+.2f}% → {macd})")
            
            # Get news and sentiment
            news = self.news_analyzer.fetch_news(company_name, symbol)
            sentiment = self.news_analyzer.analyze_sentiment(news, symbol)
            
            # Generate recommendation
            rec = self.rec_engine.generate_recommendation(
                stock_data, tech_data, sentiment, company_name
            )
            
            logger.info(f"✅ {symbol}: {rec.action.value} (Source: {rec.data_source})")
            return rec
            
        except Exception as e:
            logger.error(f"Failed to analyze {symbol}: {e}")
            return None
    
    def run_analysis(self, stocks: List[Dict[str, str]]) -> bool:
        """Run full analysis"""
        recs = []
        
        for stock in stocks:
            try:
                rec = self.analyze_stock(stock['symbol'], stock['company_name'])
                if rec:
                    recs.append(rec)
                    time.sleep(1)
            except Exception as e:
                logger.error(f"Error: {e}")
                continue
        
        if not recs:
            logger.error("No recommendations generated")
            return False
        
        self.email_sender.send_recommendation(self.recipient_email, recs)
        self.bigquery_sender.send_recommendation_v2(recs)
        return len(recs) > 0


# ===========================
# MAIN
# ===========================

def main():
    """Entry point"""
    
    GNEWS_API_KEY = os.getenv('GNEWS_API_KEY', '')
    NEWSAPI_KEY = os.getenv('NEWSAPI_KEY', '')
    OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY', '')
    EMAIL_API_URL = os.getenv('EMAIL_API_URL', '')
    RECIPIENT_EMAIL = os.getenv('RECIPIENT_EMAIL', 'your.email@example.com')
    
    STOCKS = [
        {'symbol': 'RELIANCE', 'company_name': 'Reliance Industries'},
        {'symbol': 'TCS', 'company_name': 'Tata Consultancy Services'},
        {'symbol': 'HDFCBANK', 'company_name': 'HDFC Bank'},
        {'symbol': 'INFY', 'company_name': 'Infosys'},
        {'symbol': 'ICICIBANK', 'company_name': 'ICICI Bank'},
    ]
    
    system = StockAnalysisSystem(
        gnews_key=GNEWS_API_KEY,
        newsapi_key=NEWSAPI_KEY,
        openrouter_key=OPENROUTER_API_KEY,
        email_api_url=EMAIL_API_URL,
        recipient_email=RECIPIENT_EMAIL
    )
    
    logger.info("🚀 Starting stock analysis...")
    success = system.run_analysis(STOCKS)
    
    if success:
        logger.info("✅ Analysis complete!")
    else:
        logger.error("❌ Analysis failed")


if __name__ == "__main__":
    main()
