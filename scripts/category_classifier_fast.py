import os
import warnings
warnings.filterwarnings('ignore')

try:
    from sentence_transformers import SentenceTransformer
    import numpy as np
    from sklearn.metrics.pairwise import cosine_similarity
    HAS_SENTENCE_TRANSFORMERS = True
except ImportError:
    HAS_SENTENCE_TRANSFORMERS = False

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
        
        if HAS_SENTENCE_TRANSFORMERS:
            try:
                print("⚡ Loading lightweight ML model (fast initialization)...")
                self.model = SentenceTransformer('all-MiniLM-L6-v2')  # Only 80MB, loads in seconds
                self.use_ml = True
                
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
        Backwards-compatible single-category classification.
        Internally uses the multi-category logic and returns the first match.
        """
        categories = self.classify_multi(description, merchant)
        return categories[0] if categories else "other"
    
    def classify_multi(self, description: str, merchant: str = None,
                       min_similarity: float = 0.3, max_categories: int = 3):
        """
        Multi-category classification.
        Returns a list of all matching categories (up to max_categories)
        whose similarity is above min_similarity.
        """
        if not description and not merchant:
            return []
        
        text = f"{description or ''} {merchant or ''}".strip().lower()
        if not text:
            return []
        
        # ML-based multi-label classification
        if self.use_ml and self.model and self.category_embeddings is not None:
            try:
                text_embedding = self.model.encode([text])
                similarities = cosine_similarity(text_embedding, self.category_embeddings)[0]
                
                # Take all categories above threshold, sorted by similarity
                indices = np.where(similarities > min_similarity)[0]
                if len(indices) == 0:
                    # Fallback to best single category if nothing crosses threshold
                    best_idx = int(np.argmax(similarities))
                    return [self.category_list[best_idx]]
                
                # Sort by similarity descending and cap at max_categories
                sorted_indices = indices[np.argsort(similarities[indices])[::-1]]
                selected_indices = sorted_indices[:max_categories]
                return [self.category_list[i] for i in selected_indices]
            except Exception:
                # On any ML failure, fall back to keyword logic
                return [self._keyword_classify(text)]
        
        # Pure keyword-based multi-label classification
        # We take all categories that share the maximum score (>0)
        scores = {}
        for category, keywords in CATEGORY_EXAMPLES.items():
            score = sum(1 for keyword in keywords if keyword in text)
            if score > 0:
                scores[category] = score
        
        if not scores:
            return []
        
        max_score = max(scores.values())
        return [cat for cat, score in scores.items() if score == max_score and score > 0]
    
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
        """Backwards-compatible batch interface: returns a single category per row."""
        multi = self.classify_batch_multi(descriptions, merchants)
        return [cats[0] if cats else "other" for cats in multi]
    
    def classify_batch_multi(self, descriptions, merchants=None,
                             min_similarity: float = 0.3, max_categories: int = 3):
        """Batch multi-category classification - returns list-of-lists of categories."""
        if merchants is None:
            merchants = [None] * len(descriptions)
        
        if self.use_ml and self.model and self.category_embeddings is not None:
            try:
                texts = [f"{desc} {merch or ''}".lower() for desc, merch in zip(descriptions, merchants)]
                text_embeddings = self.model.encode(texts, show_progress_bar=False, batch_size=32)
                
                similarities = cosine_similarity(text_embeddings, self.category_embeddings)
                results = []
                for row_idx in range(similarities.shape[0]):
                    sims = similarities[row_idx]
                    indices = np.where(sims > min_similarity)[0]
                    if len(indices) == 0:
                        # Fallback to best single category
                        best_idx = int(np.argmax(sims))
                        results.append([self.category_list[best_idx]])
                        continue
                    
                    sorted_indices = indices[np.argsort(sims[indices])[::-1]]
                    selected_indices = sorted_indices[:max_categories]
                    results.append([self.category_list[i] for i in selected_indices])
                
                return results
            except Exception as e:
                print(f"⚠️ Batch ML classification failed: {e}, using keyword matching")
        
        # Keyword-based multi-label for batch
        results = []
        for desc, merch in zip(descriptions, merchants):
            text = f"{desc or ''} {merch or ''}".strip().lower()
            if not text:
                results.append([])
                continue
            
            scores = {}
            for category, keywords in CATEGORY_EXAMPLES.items():
                score = sum(1 for keyword in keywords if keyword in text)
                if score > 0:
                    scores[category] = score
            
            if not scores:
                results.append([])
                continue
            
            max_score = max(scores.values())
            cats = [cat for cat, score in scores.items() if score == max_score and score > 0]
            results.append(cats)
        
        return results


_classifier_instance = None

def get_fast_classifier():
    """Get or create the global fast classifier instance"""
    global _classifier_instance
    if _classifier_instance is None:
        _classifier_instance = FastCategoryClassifier()
    return _classifier_instance

