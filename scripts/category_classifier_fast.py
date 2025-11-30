#!/usr/bin/env python3
"""
Fast Category Classifier - Optimized for Flink/Parallel Processing
Uses lightweight ML model that loads quickly and processes efficiently
"""
import os
import warnings
warnings.filterwarnings('ignore')

# Use a much faster, lighter approach
try:
    # Try to use sentence-transformers which is faster than full transformers
    from sentence_transformers import SentenceTransformer
    import numpy as np
    from sklearn.metrics.pairwise import cosine_similarity
    HAS_SENTENCE_TRANSFORMERS = True
except ImportError:
    HAS_SENTENCE_TRANSFORMERS = False

# Define transaction categories with example descriptions
CATEGORY_EXAMPLES = {
    "electronics": ["computer", "phone", "laptop", "tablet", "tv", "television", "audio", "camera", "gaming", "tech"],
    "restaurants": ["restaurant", "cafe", "coffee", "food", "dining", "pizza", "burger", "bar", "pub"],
    "travel": ["hotel", "flight", "airline", "travel", "trip", "vacation", "car rental", "taxi", "uber"],
    "entertainment": ["movie", "cinema", "theater", "concert", "show", "game", "streaming", "netflix"],
    "clothing": ["clothing", "apparel", "fashion", "shoes", "shirt", "pants", "dress", "jacket"],
    "groceries": ["grocery", "supermarket", "walmart", "target", "costco", "safeway"],
    "healthcare": ["hospital", "clinic", "doctor", "pharmacy", "medical", "health", "dental"],
    "utilities": ["electric", "water", "gas", "utility", "internet", "phone", "cable"],
    "gas_station": ["gas", "fuel", "petrol", "station", "shell", "bp", "exxon"],
    "pharmacy": ["pharmacy", "cvs", "walgreens", "rite aid", "drugstore"],
    "education": ["school", "university", "college", "tuition", "education", "course"],
    "sports": ["gym", "fitness", "sport", "athletic", "nike", "adidas"],
    "home_improvement": ["home depot", "lowes", "hardware", "furniture", "ikea"],
    "automotive": ["car", "auto", "vehicle", "tire", "repair", "mechanic"],
    "banking": ["bank", "atm", "withdrawal", "deposit", "transfer", "financial"],
    "insurance": ["insurance", "premium", "coverage"],
    "subscription": ["subscription", "monthly", "recurring", "membership"],
}

class FastCategoryClassifier:
    """
    Fast, lightweight classifier optimized for parallel processing.
    Loads model once and reuses it across all records.
    """
    
    def __init__(self):
        self.model = None
        self.category_embeddings = None
        self.use_ml = False
        
        # Try to load lightweight sentence transformer model
        if HAS_SENTENCE_TRANSFORMERS:
            try:
                print("⚡ Loading lightweight ML model (fast initialization)...")
                # Use a very small, fast model
                self.model = SentenceTransformer('all-MiniLM-L6-v2')  # Only 80MB, loads in seconds
                self.use_ml = True
                
                # Pre-compute category embeddings (do this once)
                category_texts = []
                self.category_list = []
                for cat, keywords in CATEGORY_EXAMPLES.items():
                    category_text = f"{cat} {' '.join(keywords)}"
                    category_texts.append(category_text)
                    self.category_list.append(cat)
                
                self.category_embeddings = self.model.encode(category_texts)
                print("✅ Fast ML model loaded and ready (embeddings pre-computed)")
            except Exception as e:
                print(f"⚠️ Could not load ML model: {e}")
                print("📝 Using optimized keyword matching")
                self.use_ml = False
        else:
            print("📝 Using optimized keyword matching (sentence-transformers not available)")
            self.use_ml = False
    
    def classify(self, description: str, merchant: str = None) -> str:
        """
        Fast classification - optimized for speed.
        Uses pre-computed embeddings if ML is available, otherwise keyword matching.
        """
        if not description:
            return "other"
        
        text = f"{description} {merchant or ''}".lower()
        
        if self.use_ml and self.model and self.category_embeddings is not None:
            try:
                # Encode the input text
                text_embedding = self.model.encode([text])
                
                # Find most similar category using cosine similarity
                similarities = cosine_similarity(text_embedding, self.category_embeddings)[0]
                best_match_idx = np.argmax(similarities)
                
                # Only use if similarity is above threshold
                if similarities[best_match_idx] > 0.3:
                    return self.category_list[best_match_idx]
            except Exception as e:
                # Fallback to keyword matching
                pass
        
        # Fast keyword-based classification
        return self._keyword_classify(text)
    
    def _keyword_classify(self, text: str) -> str:
        """Optimized keyword matching"""
        text_lower = text.lower()
        scores = {}
        
        for category, keywords in CATEGORY_EXAMPLES.items():
            score = sum(1 for keyword in keywords if keyword in text_lower)
            if score > 0:
                scores[category] = score
        
        if scores:
            return max(scores.items(), key=lambda x: x[1])[0]
        
        return "other"
    
    def classify_batch(self, descriptions, merchants=None):
        """Batch classification - optimized for parallel processing"""
        if merchants is None:
            merchants = [None] * len(descriptions)
        
        if self.use_ml and self.model and self.category_embeddings is not None:
            try:
                # Batch encode for efficiency
                texts = [f"{desc} {merch or ''}".lower() for desc, merch in zip(descriptions, merchants)]
                text_embeddings = self.model.encode(texts, show_progress_bar=False, batch_size=32)
                
                # Batch similarity computation
                similarities = cosine_similarity(text_embeddings, self.category_embeddings)
                best_matches = np.argmax(similarities, axis=1)
                max_similarities = np.max(similarities, axis=1)
                
                # Apply threshold
                results = []
                for idx, sim in zip(best_matches, max_similarities):
                    if sim > 0.3:
                        results.append(self.category_list[idx])
                    else:
                        results.append(self._keyword_classify(texts[len(results)]))
                
                return results
            except Exception as e:
                print(f"⚠️ Batch ML classification failed: {e}, using keyword matching")
        
        # Fallback to keyword matching
        return [self._keyword_classify(f"{desc} {merch or ''}".lower()) 
                for desc, merch in zip(descriptions, merchants)]


# Global instance for reuse
_classifier_instance = None

def get_fast_classifier():
    """Get or create the global fast classifier instance"""
    global _classifier_instance
    if _classifier_instance is None:
        _classifier_instance = FastCategoryClassifier()
    return _classifier_instance

