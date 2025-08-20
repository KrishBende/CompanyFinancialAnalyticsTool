import re
from typing import Dict, List, Tuple, Optional

class ImprovedNLPProcessor:
    """
    An improved NLP processor for parsing financial queries with better pattern matching.
    """
    
    def __init__(self):
        """
        Initializes the processor with keywords and patterns.
        """
        # Define keywords for different actions
        self.action_keywords = {
            'show': ['show', 'display', 'list', 'view', 'give', 'provide', 'what is', 'what are'],
            'plot': ['plot', 'chart', 'graph', 'visualize', 'draw', 'illustrate', 'show me a chart'],
            'total': ['total', 'sum', 'aggregate', 'overall', 'combined', 'altogether'],
            'average': ['average', 'mean', 'avg', 'median'],
            'trend': ['trend', 'pattern', 'over time', 'growth', 'change', 'evolution'],
            'predict': ['predict', 'forecast', 'future', 'estimate', 'project', 'will'],
            'compare': ['compare', 'versus', 'vs', 'difference', 'contrast', 'versus']
        }
        
        # Define financial metrics
        self.metric_keywords = {
            'sales': ['sales', 'revenue', 'income', 'earnings'],
            'profit': ['profit', 'earnings', 'gain', 'margin', 'net income'],
            'quantity': ['quantity', 'units sold', 'volume', 'amount', 'count', 'number', 'sold'],
            'discount': ['discount', 'reduction', 'off', 'markdown']
        }
        
        # Define dimensions
        self.dimension_keywords = {
            'category': ['category', 'product category', 'type', 'kind'],
            'region': ['region', 'area', 'location', 'zone', 'geography'],
            'segment': ['segment', 'customer segment', 'group', 'demographic'],
            'time': ['year', 'month', 'quarter', 'week', 'date', 'time', 'period']
        }
        
        # Define dimension indicators
        self.dimension_indicators = {
            'category': ['by category', 'per category', 'in each category', 'for each category'],
            'region': ['by region', 'per region', 'in each region', 'for each region', 'by area', 'per area'],
            'segment': ['by segment', 'per segment', 'in each segment', 'for each segment'],
            'time': ['by year', 'per year', 'over time', 'by month', 'per month', 'by quarter', 'per quarter']
        }
        
        # Countries for filtering
        self.common_countries = [
            'united states', 'usa', 'us', 'canada', 'mexico', 'brazil', 'argentina', 'chile',
            'uk', 'united kingdom', 'france', 'germany', 'italy', 'spain', 'netherlands',
            'sweden', 'norway', 'denmark', 'finland', 'russia', 'china', 'japan', 'india',
            'australia', 'new zealand', 'south africa', 'egypt', 'nigeria', 'kenya'
        ]
        
    def _extract_action(self, query: str) -> str:
        """Extract action from query."""
        query_lower = query.lower()
        
        # Check for specific action keywords
        for action, keywords in self.action_keywords.items():
            if any(keyword in query_lower for keyword in keywords):
                # Special case for "what is the average" pattern
                if action == 'show' and 'average' in query_lower:
                    return 'average'
                # Special case for "what is the total" pattern
                if action == 'show' and any(word in query_lower for word in ['total', 'sum']):
                    return 'total'
                return action
                
        # Default to 'show' if no action is found
        return 'show'
    
    def _extract_metric(self, query: str) -> Optional[str]:
        """Extract metric from query."""
        query_lower = query.lower()
        
        # Look for metrics in order of specificity to avoid conflicts
        # Check for most specific first
        if any(keyword in query_lower for keyword in self.metric_keywords['discount']):
            return 'discount'
        if any(keyword in query_lower for keyword in self.metric_keywords['profit']):
            return 'profit'
        if any(keyword in query_lower for keyword in self.metric_keywords['sales']):
            return 'sales'
        if any(keyword in query_lower for keyword in self.metric_keywords['quantity']):
            return 'quantity'
                
        return None
    
    def _extract_dimension(self, query: str) -> Optional[str]:
        """Extract dimension from query."""
        query_lower = query.lower()
        
        # Look for dimension indicators (more specific patterns)
        for dimension, indicators in self.dimension_indicators.items():
            if any(indicator in query_lower for indicator in indicators):
                return dimension
                
        # Look for general dimension keywords
        for dimension, keywords in self.dimension_keywords.items():
            if any(keyword in query_lower for keyword in keywords):
                # Special handling for "per X" patterns
                if dimension == 'category' and 'per' in query_lower:
                    # Check if "per" is followed by a category-related word
                    if re.search(r'per\s+(category|type|kind)', query_lower):
                        return 'category'
                elif dimension == 'region' and 'per' in query_lower:
                    # Check if "per" is followed by a region-related word
                    if re.search(r'per\s+(region|area|location)', query_lower):
                        return 'region'
                elif dimension == 'segment' and 'per' in query_lower:
                    # Check if "per" is followed by a segment-related word
                    if re.search(r'per\s+(segment|group)', query_lower):
                        return 'segment'
                elif dimension == 'time' and 'per' in query_lower:
                    # Check if "per" is followed by a time-related word
                    if re.search(r'per\s+(year|month|quarter)', query_lower):
                        return 'time'
                else:
                    return dimension
                    
        # Special case for "per country" pattern
        if 'per country' in query_lower:
            return 'region'  # Treat country as region for our data
            
        return None
    
    def _extract_filters(self, query: str) -> Dict[str, str]:
        """Extract filters from query."""
        filters = {}
        query_lower = query.lower()
        
        # Extract region/country filters
        if 'in the' in query_lower:
            # Look for patterns like "in the United States"
            country_match = re.search(r'in the\s+([a-zA-Z\s]+?)(?:\s+(?:in|by|and|or|,|\.|$))', query_lower)
            if country_match:
                country = country_match.group(1).strip()
                if country and len(country) > 1:
                    filters['region'] = country.title()
                    
        # Look for specific country names
        for country in self.common_countries:
            if country in query_lower:
                filters['region'] = country.title()
                break
                
        return filters
    
    def _extract_time_range(self, query: str) -> Optional[Dict[str, int]]:
        """Extract time range from query."""
        query_lower = query.lower()
        
        # Extract years
        year_matches = re.findall(r'\b(20\d{2})\b', query_lower)
        if year_matches:
            years = sorted([int(y) for y in year_matches])
            if len(years) == 1:
                return {'year': years[0]}
            elif len(years) >= 2:
                return {'start_year': years[0], 'end_year': years[-1]}
                
        return None
    
    def parse_query(self, query: str) -> Dict:
        """
        Parses a natural language query using improved pattern matching.
        
        Args:
            query (str): The user's natural language query.
            
        Returns:
            dict: A dictionary containing parsed components of the query.
        """
        # Initialize parsed components
        parsed = {
            'original_query': query,
            'action': None,
            'metric': None,
            'dimension': None,
            'filters': {},
            'time_range': None,
            'predicted': False
        }
        
        # Extract components
        parsed['action'] = self._extract_action(query)
        parsed['metric'] = self._extract_metric(query)
        parsed['dimension'] = self._extract_dimension(query)
        parsed['filters'] = self._extract_filters(query)
        parsed['time_range'] = self._extract_time_range(query)
        
        # Special handling for prediction queries
        if 'predict' in query.lower() or 'forecast' in query.lower() or 'will' in query.lower():
            parsed['predicted'] = True
            parsed['action'] = 'predict'
            
        return parsed


# Example usage and testing
if __name__ == "__main__":
    processor = ImprovedNLPProcessor()
    
    # Test queries
    test_queries = [
        "show me average profit per country",
        "Show me total sales by category",
        "Plot the trend of profit over time",
        "What is the average discount by segment?",
        "Show me total sales in the West region for 2012",
        "What was the profit trend from 2011 to 2013?",
        "Predict sales for next year",
        "Compare sales in the United States",
        "What is the total quantity sold by region?",
        "Display average sales per month"
    ]
    
    for query in test_queries:
        result = processor.parse_query(query)
        print(f"Query: {query}")
        print(f"Parsed: {result}")
        print("-" * 50)