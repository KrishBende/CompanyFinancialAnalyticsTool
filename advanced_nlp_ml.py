import spacy
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import pandas as pd
import numpy as np

class AdvancedNLPProcessor:
    """
    An advanced NLP processor using spaCy for more sophisticated natural language understanding.
    """
    
    def __init__(self):
        """
        Initializes the spaCy NLP model.
        """
        try:
            self.nlp = spacy.load("en_core_web_sm")
            print("spaCy model loaded successfully.")
        except OSError:
            print("Error: Could not load spaCy model 'en_core_web_sm'. Please install it using 'python -m spacy download en_core_web_sm'")
            self.nlp = None
            
    def parse_query(self, query):
        """
        Parses a natural language query using spaCy for more advanced NLP processing.
        
        Args:
            query (str): The user's natural language query.
            
        Returns:
            dict: A dictionary containing parsed components of the query.
        """
        if not self.nlp:
            # Fallback to basic parsing if spaCy model is not available
            return self._basic_parse(query)
            
        # Process the query with spaCy
        doc = self.nlp(query)
        
        # Initialize parsed components
        parsed = {
            'original_query': query,
            'action': None,
            'metric': None,
            'dimension': None,
            'filters': {},
            'time_range': None,
            'entities': [],
            'predicted': False
        }
        
        # Extract named entities
        for ent in doc.ents:
            parsed['entities'].append({
                'text': ent.text,
                'label': ent.label_,
                'start': ent.start_char,
                'end': ent.end_char
            })
            
        # Extract noun phrases
        noun_phrases = [chunk.text.lower() for chunk in doc.noun_chunks]
        
        # Extract tokens (lemmatized)
        tokens = [token.lemma_.lower() for token in doc if not token.is_stop and not token.is_punct]
        
        # Also check the full query text for keywords
        query_lower = query.lower()
        
        # Define keywords for different actions
        action_keywords = {
            'show': ['show', 'display', 'list', 'view', 'give', 'provide'],
            'plot': ['plot', 'chart', 'graph', 'visualize', 'draw', 'illustrate'],
            'total': ['total', 'sum', 'aggregate', 'overall', 'combined'],
            'average': ['average', 'mean', 'avg', 'median'],
            'trend': ['trend', 'pattern', 'over time', 'growth', 'change'],
            'predict': ['predict', 'forecast', 'future', 'estimate', 'project'],
            'compare': ['compare', 'versus', 'vs', 'difference', 'contrast']
        }
        
        # Define financial metrics
        metric_keywords = {
            'sales': ['sales', 'revenue', 'income', 'earnings'],
            'profit': ['profit', 'earnings', 'gain', 'margin'],
            'quantity': ['quantity', 'units sold', 'volume', 'amount'],
            'discount': ['discount', 'reduction', 'off']
        }
        
        # Define dimensions
        dimension_keywords = {
            'category': ['category', 'product category', 'type'],
            'region': ['region', 'area', 'location', 'zone'],
            'segment': ['segment', 'customer segment', 'group'],
            'time': ['year', 'month', 'quarter', 'week', 'date', 'time']
        }
        
        # Determine action
        for action, keywords in action_keywords.items():
            # Check in tokens, noun phrases, and full query
            if (any(token in keywords for token in tokens) or 
                any(np in keywords for np in noun_phrases) or
                any(keyword in query_lower for keyword in keywords)):
                parsed['action'] = action
                if action == 'predict':
                    parsed['predicted'] = True
                break
                
        # Determine metric
        for metric, keywords in metric_keywords.items():
            # Check in tokens, noun phrases, and full query
            if (any(token in keywords for token in tokens) or 
                any(np in keywords for np in noun_phrases) or
                any(keyword in query_lower for keyword in keywords)):
                parsed['metric'] = metric
                break
                
        # Determine dimension
        for dimension, keywords in dimension_keywords.items():
            # Check in tokens, noun phrases, and full query
            if (any(token in keywords for token in tokens) or 
                any(np in keywords for np in noun_phrases) or
                any(keyword in query_lower for keyword in keywords)):
                parsed['dimension'] = dimension
                break
                
        # Extract time-related information
        time_entities = [ent for ent in doc.ents if ent.label_ in ['DATE', 'TIME']]
        if time_entities:
            # This is a simplified approach to extracting years
            # In a more robust implementation, you'd use date parsing libraries
            for ent in time_entities:
                if ent.text.isdigit() and len(ent.text) == 4:
                    year = int(ent.text)
                    if 2000 <= year <= 2030:  # Reasonable year range
                        parsed['time_range'] = {'year': year}
                        break
                        
        # Extract filter information from entities
        gpe_entities = [ent for ent in doc.ents if ent.label_ == 'GPE']  # Geopolitical entities (countries, cities, states)
        org_entities = [ent for ent in doc.ents if ent.label_ == 'ORG']  # Organizations
        
        # Add GPE entities as potential region filters
        if gpe_entities:
            parsed['filters']['region'] = gpe_entities[0].text
            
        # Add ORG entities as potential category filters (simplified)
        if org_entities:
            parsed['filters']['category'] = org_entities[0].text
            
        return parsed
    
    def _basic_parse(self, query):
        """
        A fallback basic parser in case spaCy model is not available.
        This is a simplified version of the regex-based parser from earlier.
        """
        import re
        
        lower_query = query.lower().strip()
        
        parsed = {
            'original_query': query,
            'action': None,
            'metric': None,
            'dimension': None,
            'filters': {},
            'time_range': None,
            'entities': [],
            'predicted': False
        }
        
        # Define keywords for different actions
        action_keywords = {
            'show': ['show', 'display', 'list', 'view', 'give', 'provide'],
            'plot': ['plot', 'chart', 'graph', 'visualize', 'draw', 'illustrate'],
            'total': ['total', 'sum', 'aggregate', 'overall', 'combined'],
            'average': ['average', 'mean', 'avg', 'median'],
            'trend': ['trend', 'pattern', 'over time', 'growth', 'change'],
            'predict': ['predict', 'forecast', 'future', 'estimate', 'project'],
            'compare': ['compare', 'versus', 'vs', 'difference', 'contrast']
        }
        
        # Define financial metrics
        metric_keywords = {
            'sales': ['sales', 'revenue', 'income', 'earnings'],
            'profit': ['profit', 'earnings', 'gain', 'margin'],
            'quantity': ['quantity', 'units sold', 'volume', 'amount'],
            'discount': ['discount', 'reduction', 'off']
        }
        
        # Define dimensions
        dimension_keywords = {
            'category': ['category', 'product category', 'type'],
            'region': ['region', 'area', 'location', 'zone'],
            'segment': ['segment', 'customer segment', 'group'],
            'time': ['year', 'month', 'quarter', 'week', 'date', 'time']
        }
        
        # Determine action
        for action, keywords in action_keywords.items():
            if any(keyword in lower_query for keyword in keywords):
                parsed['action'] = action
                if action == 'predict':
                    parsed['predicted'] = True
                break
                
        # Determine metric
        for metric, keywords in metric_keywords.items():
            if any(keyword in lower_query for keyword in keywords):
                parsed['metric'] = metric
                break
                
        # Determine dimension
        for dimension, keywords in dimension_keywords.items():
            if any(keyword in lower_query for keyword in keywords):
                parsed['dimension'] = dimension
                break
                
        # Extract time range (simple pattern matching)
        year_matches = re.findall(r'\b(20\d{2})\b', lower_query)
        if year_matches:
            years = sorted([int(y) for y in year_matches])
            if len(years) == 1:
                parsed['time_range'] = {'year': years[0]}
            elif len(years) >= 2:
                parsed['time_range'] = {'start_year': years[0], 'end_year': years[-1]}
                
        # Extract filters for region
        region_patterns = [
            r'\b(in|for|from)\s+([a-zA-Z\s]+?)(?:\s+(?:in|by|and|or|,|\.|$))',
            r'\b(region)\s+([a-zA-Z\s]+?)(?:\s+(?:in|by|and|or|,|\.|$))'
        ]
        
        for pattern in region_patterns:
            region_match = re.search(pattern, lower_query)
            if region_match:
                region = region_match.group(2).strip()
                if region and len(region) > 1 and region not in ['the', 'a', 'an']:
                    parsed['filters']['region'] = region.title()
                break
                
        # Extract filters for category
        category_patterns = [
            r'\b(by|in|for)\s+([a-zA-Z\s]+?)(?:\s+(?:in|by|and|or|,|\.|$))',
            r'\b(category)\s+([a-zA-Z\s]+?)(?:\s+(?:in|by|and|or|,|\.|$))'
        ]
        
        for pattern in category_patterns:
            category_match = re.search(pattern, lower_query)
            if category_match:
                category = category_match.group(2).strip()
                if category and len(category) > 1 and category not in ['the', 'a', 'an']:
                    parsed['filters']['category'] = category.title()
                break
                
        return parsed

class MLProcessor:
    """
    A machine learning processor for making predictions on financial data.
    """
    
    def __init__(self):
        """
        Initializes the ML processor.
        """
        pass
    
    def predict_future_sales(self, df, years_to_predict=3):
        """
        Predicts future sales using linear regression.
        
        Args:
            df (pyspark.sql.DataFrame): The Spark DataFrame containing historical data.
            years_to_predict (int): Number of years to predict into the future.
            
        Returns:
            dict: A dictionary containing the predictions and model metrics.
        """
        # Convert to Pandas for ML processing
        pandas_df = df.toPandas()
        
        # Group by year and sum sales
        yearly_sales = pandas_df.groupby('Year')['Sales'].sum().reset_index()
        
        # Prepare data for linear regression
        X = yearly_sales['Year'].values.reshape(-1, 1)
        y = yearly_sales['Sales'].values
        
        # Split data for training and testing (using last 20% for testing)
        split_index = int(len(X) * 0.8)
        X_train, X_test = X[:split_index], X[split_index:]
        y_train, y_test = y[:split_index], y[split_index:]
        
        # Train linear regression model
        model = LinearRegression()
        model.fit(X_train, y_train)
        
        # Make predictions on test set
        y_pred = model.predict(X_test)
        
        # Calculate metrics
        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)
        
        # Predict future years
        last_year = yearly_sales['Year'].max()
        future_years = np.array([last_year + i for i in range(1, years_to_predict + 1)]).reshape(-1, 1)
        future_sales = model.predict(future_years)
        
        # Create results dictionary
        results = {
            'model': 'Linear Regression',
            'metrics': {
                'mse': mse,
                'rmse': rmse
            },
            'historical_data': yearly_sales.to_dict('records'),
            'predictions': [
                {'year': int(future_years[i][0]), 'predicted_sales': float(future_sales[i])}
                for i in range(len(future_years))
            ]
        }
        
        return results

# Example usage (for testing)
if __name__ == "__main__":
    # Test the AdvancedNLPProcessor
    nlp_processor = AdvancedNLPProcessor()
    
    # Test queries
    test_queries = [
        "Show me total sales by category in 2011",
        "Plot the trend of profit over time",
        "Predict sales for next year",
        "What is the average discount by segment?",
        "Compare sales in the United States"
    ]
    
    for query in test_queries:
        print(f"Query: {query}")
        result = nlp_processor.parse_query(query)
        print(f"Parsed: {result}")
        print("-" * 50)