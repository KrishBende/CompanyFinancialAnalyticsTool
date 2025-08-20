# This file is deprecated in favor of advanced_nlp_ml.py which uses spaCy
# Keeping it for reference or fallback purposes

import re
from datetime import datetime

class NLPQueryParser:
    """
    A simple NLP query parser to interpret user requests for financial data analysis.
    This is a basic implementation using regex and keyword matching.
    """
    
    def __init__(self):
        """
        Initializes the parser with common keywords and patterns.
        """
        # Define common keywords for different actions
        self.keywords = {
            'show': ['show', 'display', 'list', 'view'],
            'plot': ['plot', 'chart', 'graph', 'visualize', 'draw'],
            'total': ['total', 'sum', 'aggregate', 'overall'],
            'average': ['average', 'mean', 'avg'],
            'trend': ['trend', 'pattern', 'over time', 'growth'],
            'predict': ['predict', 'forecast', 'future'],
            'compare': ['compare', 'versus', 'vs', 'difference']
        }
        
        # Define common financial metrics
        self.metrics = {
            'sales': ['sales', 'revenue'],
            'profit': ['profit', 'earnings', 'income'],
            'quantity': ['quantity', 'units sold', 'volume'],
            'discount': ['discount']
        }
        
        # Define common dimensions/groupings
        self.dimensions = {
            'category': ['category', 'product category'],
            'region': ['region', 'area'],
            'segment': ['segment', 'customer segment'],
            'time': ['year', 'month', 'quarter', 'week', 'date']
        }
        
    def parse_query(self, query):
        """
        Parses a natural language query into structured components.
        
        Args:
            query (str): The user's natural language query.
            
        Returns:
            dict: A dictionary containing parsed components of the query.
        """
        # Convert query to lowercase for easier matching
        lower_query = query.lower().strip()
        
        # Initialize parsed components
        parsed = {
            'original_query': query,
            'action': None,
            'metric': None,
            'dimension': None,
            'filters': {},
            'time_range': None,
            'comparison': None
        }
        
        # Determine action
        for action, keywords in self.keywords.items():
            if any(keyword in lower_query for keyword in keywords):
                parsed['action'] = action
                break
                
        # Determine metric
        for metric, keywords in self.metrics.items():
            if any(keyword in lower_query for keyword in keywords):
                parsed['metric'] = metric
                break
                
        # Determine dimension
        for dimension, keywords in self.dimensions.items():
            if any(keyword in lower_query for keyword in keywords):
                parsed['dimension'] = dimension
                break
                
        # Extract time range (simple pattern matching for now)
        # Looking for years in the query
        year_matches = re.findall(r'\b(20\d{2})\b', lower_query)
        if year_matches:
            # Convert to integers and sort
            years = sorted([int(y) for y in year_matches])
            if len(years) == 1:
                parsed['time_range'] = {'year': years[0]}
            elif len(years) >= 2:
                parsed['time_range'] = {'start_year': years[0], 'end_year': years[-1]}
                
        # Extract filters for region
        # This is a very simplified approach - in a real system, you'd have a list of known regions
        region_patterns = [
            r'\b(in|for|from)\s+([a-zA-Z\s]+?)(?:\s+(?:in|by|and|or|,|\.|$))',
            r'\b(region)\s+([a-zA-Z\s]+?)(?:\s+(?:in|by|and|or|,|\.|$))'
        ]
        
        for pattern in region_patterns:
            region_match = re.search(pattern, lower_query)
            if region_match:
                region = region_match.group(2).strip()
                # Basic validation to avoid capturing stop words
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
                # Basic validation
                if category and len(category) > 1 and category not in ['the', 'a', 'an']:
                    parsed['filters']['category'] = category.title()
                break
                
        # Handle comparison queries
        compare_match = re.search(r'compare\s+(.+?)\s+(vs|versus)\s+(.+)', lower_query)
        if compare_match:
            parsed['comparison'] = {
                'item1': compare_match.group(1).strip(),
                'item2': compare_match.group(3).strip()
            }
            
        return parsed

# Example usage (for testing)
if __name__ == "__main__":
    parser = NLPQueryParser()
    
    # Test queries
    test_queries = [
        "Show me total sales by category in 2011",
        "Plot the trend of profit over time",
        "What is the average discount by segment?",
        "Predict sales for next year",
        "Compare sales in the West region vs East region",
        "Show me total sales in the West region for 2012",
        "What was the profit trend from 2011 to 2013?",
        "Display average quantity by category"
    ]
    
    for query in test_queries:
        print(f"Query: {query}")
        result = parser.parse_query(query)
        print(f"Parsed: {result}")
        print("-" * 50)