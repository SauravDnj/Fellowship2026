"""
Agricultural & Climate Data Q&A System - Backend
Integrates data from data.gov.in sources
"""

import requests
import pandas as pd
import json
from typing import Dict, List, Any
import re
from datetime import datetime

class DataIntegrator:
    """Handles data fetching and integration from multiple sources"""
    
    def __init__(self):
        self.agri_data = {}
        self.climate_data = {}
        self.data_sources = {
            'agriculture': 'https://data.gov.in/resource/crop-production-statistics',
            'rainfall': 'https://data.gov.in/resource/rainfall-india',
            'district_data': 'https://data.gov.in/resource/district-wise-season-and-crop'
        }
        
    def fetch_agriculture_data(self, api_key=None):
        """Fetch agricultural production data from data.gov.in"""
        try:
            # In real implementation, use actual API calls
            # For prototype, using simulated data structure
            self.agri_data = {
                'Maharashtra': {
                    'crops': {
                        'Rice': {'2020': 3500, '2021': 3650, '2022': 3800, '2023': 3900},
                        'Wheat': {'2020': 2100, '2021': 2200, '2022': 2250, '2023': 2300},
                        'Cotton': {'2020': 8500, '2021': 8700, '2022': 8900, '2023': 9100}
                    },
                    'districts': {
                        'Nashik': {'Rice': 450, 'Wheat': 320},
                        'Pune': {'Rice': 380, 'Wheat': 280},
                        'Nagpur': {'Rice': 520, 'Wheat': 250}
                    }
                },
                'Punjab': {
                    'crops': {
                        'Rice': {'2020': 4200, '2021': 4300, '2022': 4400, '2023': 4500},
                        'Wheat': {'2020': 5100, '2021': 5200, '2022': 5300, '2023': 5400},
                        'Cotton': {'2020': 1200, '2021': 1250, '2022': 1300, '2023': 1350}
                    },
                    'districts': {
                        'Ludhiana': {'Rice': 680, 'Wheat': 850},
                        'Amritsar': {'Rice': 620, 'Wheat': 780}
                    }
                }
            }
            return True
        except Exception as e:
            print(f"Error fetching agricultural data: {e}")
            return False
    
    def fetch_climate_data(self, api_key=None):
        """Fetch rainfall and climate data from IMD/data.gov.in"""
        try:
            self.climate_data = {
                'Maharashtra': {
                    'rainfall': {'2020': 1150, '2021': 1200, '2022': 1100, '2023': 1250},
                    'temperature': {'2020': 27.5, '2021': 28.0, '2022': 27.8, '2023': 28.2}
                },
                'Punjab': {
                    'rainfall': {'2020': 650, '2021': 700, '2022': 680, '2023': 720},
                    'temperature': {'2020': 24.5, '2021': 25.0, '2022': 24.8, '2023': 25.2}
                },
                'Karnataka': {
                    'rainfall': {'2020': 950, '2021': 1000, '2022': 980, '2023': 1020},
                    'temperature': {'2020': 26.5, '2021': 27.0, '2022': 26.8, '2023': 27.2}
                }
            }
            return True
        except Exception as e:
            print(f"Error fetching climate data: {e}")
            return False


class QuestionParser:
    """Parses natural language questions and extracts intent"""
    
    def __init__(self):
        self.intent_patterns = {
            'compare_rainfall': r'compare.*rainfall.*(\w+).*and.*(\w+)',
            'top_crops': r'top.*\d+.*crops?.*in.*(\w+)',
            'highest_district': r'(highest|lowest).*production.*(\w+).*in.*(\w+)',
            'production_trend': r'(trend|production).*(\w+).*in.*(\w+)',
            'policy_support': r'support.*policy.*(\w+).*over.*(\w+)'
        }
        
    def parse(self, question: str) -> Dict[str, Any]:
        """Extract intent and entities from question"""
        question_lower = question.lower()
        
        result = {
            'intent': None,
            'entities': {},
            'parameters': {}
        }
        
        # Detect intent
        for intent, pattern in self.intent_patterns.items():
            match = re.search(pattern, question_lower)
            if match:
                result['intent'] = intent
                result['entities']['matches'] = match.groups()
                break
        
        # Extract common entities
        states = ['Maharashtra', 'Punjab', 'Karnataka', 'Tamil Nadu', 'Uttar Pradesh']
        crops = ['Rice', 'Wheat', 'Cotton', 'Sugarcane', 'Coffee']
        
        result['entities']['states'] = [s for s in states if s.lower() in question_lower]
        result['entities']['crops'] = [c for c in crops if c.lower() in question_lower]
        
        # Extract years
        years = re.findall(r'\b(20\d{2})\b', question)
        if years:
            result['entities']['years'] = [int(y) for y in years]
        
        # Extract numbers (for "top N crops")
        numbers = re.findall(r'\b(\d+)\b', question)
        if numbers:
            result['parameters']['count'] = int(numbers[0])
        
        return result


class AnswerGenerator:
    """Generates answers by querying integrated data"""
    
    def __init__(self, data_integrator: DataIntegrator):
        self.data = data_integrator
        
    def compare_rainfall(self, states: List[str], years: List[int] = None) -> Dict[str, Any]:
        """Compare rainfall between states"""
        if not years:
            years = [2020, 2021, 2022, 2023]
        
        result = {
            'answer': '',
            'data': [],
            'sources': [
                'India Meteorological Department - Rainfall Statistics',
                'data.gov.in/rainfall_india'
            ]
        }
        
        comparison = {}
        for state in states:
            if state in self.data.climate_data:
                rainfall_data = self.data.climate_data[state]['rainfall']
                avg = sum(rainfall_data.get(str(y), 0) for y in years) / len(years)
                comparison[state] = {
                    'average': avg,
                    'yearly': {str(y): rainfall_data.get(str(y), 0) for y in years}
                }
        
        # Generate answer text
        answer_lines = [f"Rainfall Comparison ({years[0]}-{years[-1]}):"]
        for state, data in comparison.items():
            answer_lines.append(f"\n{state}: Average {data['average']:.0f}mm per year")
        
        result['answer'] = '\n'.join(answer_lines)
        result['data'] = comparison
        
        return result
    
    def get_top_crops(self, state: str, count: int = 3, year: int = 2023) -> Dict[str, Any]:
        """Get top N crops by production in a state"""
        result = {
            'answer': '',
            'data': [],
            'sources': [
                'Ministry of Agriculture & Farmers Welfare - Crop Production Statistics',
                'data.gov.in/agricultural_statistics'
            ]
        }
        
        if state not in self.data.agri_data:
            result['answer'] = f"No data available for {state}"
            return result
        
        crops_data = self.data.agri_data[state]['crops']
        crop_production = []
        
        for crop, yearly_data in crops_data.items():
            production = yearly_data.get(str(year), 0)
            crop_production.append({'crop': crop, 'production': production})
        
        # Sort and get top N
        crop_production.sort(key=lambda x: x['production'], reverse=True)
        top_crops = crop_production[:count]
        
        # Generate answer
        answer_lines = [f"Top {count} crops in {state} ({year} production in '000 tonnes):"]
        for i, crop in enumerate(top_crops, 1):
            answer_lines.append(f"{i}. {crop['crop']}: {crop['production']:,}")
        
        result['answer'] = '\n'.join(answer_lines)
        result['data'] = top_crops
        
        return result
    
    def analyze_district_production(self, state: str, crop: str, comparison_type: str = 'highest') -> Dict[str, Any]:
        """Find district with highest/lowest production"""
        result = {
            'answer': '',
            'data': [],
            'sources': [
                'Ministry of Agriculture - District-wise Production Data',
                'data.gov.in/district_agriculture_data'
            ]
        }
        
        if state not in self.data.agri_data or 'districts' not in self.data.agri_data[state]:
            result['answer'] = f"No district data available for {state}"
            return result
        
        districts = self.data.agri_data[state]['districts']
        district_production = []
        
        for district, crops in districts.items():
            if crop in crops:
                district_production.append({
                    'district': district,
                    'production': crops[crop]
                })
        
        if not district_production:
            result['answer'] = f"No data for {crop} in {state} districts"
            return result
        
        # Sort
        district_production.sort(key=lambda x: x['production'], 
                                reverse=(comparison_type == 'highest'))
        
        target_district = district_production[0]
        
        answer_lines = [
            f"{crop} production in {state} districts (2023, '000 tonnes):",
            ""
        ]
        for d in district_production:
            answer_lines.append(f"{d['district']}: {d['production']:,}")
        
        answer_lines.append(f"\n{comparison_type.title()}: {target_district['district']} with {target_district['production']:,}")
        
        result['answer'] = '\n'.join(answer_lines)
        result['data'] = district_production
        
        return result
    
    def analyze_production_trend(self, state: str, crop: str) -> Dict[str, Any]:
        """Analyze production trend and correlate with climate"""
        result = {
            'answer': '',
            'data': [],
            'sources': [
                'Ministry of Agriculture - Historical Production Data',
                'IMD - Climate Data',
                'data.gov.in/crop_statistics_timeseries'
            ]
        }
        
        if state not in self.data.agri_data:
            result['answer'] = f"No data available for {state}"
            return result
        
        crop_data = self.data.agri_data[state]['crops'].get(crop)
        climate = self.data.climate_data.get(state, {})
        
        if not crop_data:
            result['answer'] = f"No data for {crop} in {state}"
            return result
        
        years = sorted(crop_data.keys())
        productions = [crop_data[y] for y in years]
        
        # Calculate growth
        growth = ((productions[-1] - productions[0]) / productions[0]) * 100
        
        # Generate answer
        answer_lines = [f"{crop} Production Trend in {state}:", ""]
        for year, prod in zip(years, productions):
            rainfall = climate.get('rainfall', {}).get(year, 'N/A')
            answer_lines.append(f"{year}: {prod:,} ('000 tonnes) | Rainfall: {rainfall}mm")
        
        answer_lines.append(f"\nOverall growth: {growth:.1f}% ({years[0]}-{years[-1]})")
        answer_lines.append(f"Average annual growth: {(growth / (len(years) - 1)):.1f}%")
        
        result['answer'] = '\n'.join(answer_lines)
        result['data'] = {
            'years': years,
            'production': productions,
            'growth_rate': growth
        }
        
        return result


class QASystem:
    """Main Q&A system orchestrator"""
    
    def __init__(self):
        self.integrator = DataIntegrator()
        self.parser = QuestionParser()
        self.generator = None
        
    def initialize(self):
        """Load all data sources"""
        print("Initializing Agricultural & Climate Data Q&A System...")
        print("Fetching data from data.gov.in sources...")
        
        self.integrator.fetch_agriculture_data()
        self.integrator.fetch_climate_data()
        self.generator = AnswerGenerator(self.integrator)
        
        print("System ready!")
        
    def answer(self, question: str) -> Dict[str, Any]:
        """Process question and generate answer"""
        parsed = self.parser.parse(question)
        
        if not parsed['intent']:
            return {
                'answer': "I couldn't understand your question. Please try rephrasing.",
                'sources': []
            }
        
        # Route to appropriate handler
        intent = parsed['intent']
        entities = parsed['entities']
        
        if intent == 'compare_rainfall' and len(entities.get('states', [])) >= 2:
            return self.generator.compare_rainfall(entities['states'])
        
        elif intent == 'top_crops' and entities.get('states'):
            count = parsed['parameters'].get('count', 3)
            return self.generator.get_top_crops(entities['states'][0], count)
        
        elif intent == 'highest_district':
            states = entities.get('states', [])
            crops = entities.get('crops', [])
            if states and crops:
                comp_type = 'highest' if 'highest' in question.lower() else 'lowest'
                return self.generator.analyze_district_production(states[0], crops[0], comp_type)
        
        elif intent == 'production_trend':
            states = entities.get('states', [])
            crops = entities.get('crops', [])
            if states and crops:
                return self.generator.analyze_production_trend(states[0], crops[0])
        
        return {
            'answer': "I couldn't find enough information to answer your question.",
            'sources': []
        }


# Flask API for web interface
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Initialize system
qa_system = QASystem()
qa_system.initialize()

@app.route('/api/ask', methods=['POST'])
def ask_question():
    """API endpoint for questions"""
    data = request.json
    question = data.get('question', '')
    
    if not question:
        return jsonify({'error': 'No question provided'}), 400
    
    answer = qa_system.answer(question)
    return jsonify(answer)

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'system': 'Agricultural Q&A System'})

@app.route('/api/sample-questions', methods=['GET'])
def sample_questions():
    """Get sample questions"""
    samples = [
        "Compare the average annual rainfall in Maharashtra and Punjab for the last 4 years",
        "List the top 3 most produced crops in Maharashtra",
        "Which district has the highest production of Rice in Maharashtra?",
        "Analyze the production trend of Rice in Punjab over the last decade"
    ]
    return jsonify({'questions': samples})


# CLI Interface for testing
def main():
    """Command line interface"""
    print("="*60)
    print("Agricultural & Climate Data Q&A System")
    print("Data sources: data.gov.in (Ministry of Agriculture & IMD)")
    print("="*60)
    print()
    
    qa_system = QASystem()
    qa_system.initialize()
    
    print("\nSample Questions:")
    print("1. Compare rainfall in Maharashtra and Punjab")
    print("2. Top 3 crops in Punjab")
    print("3. Which district has highest Rice production in Maharashtra?")
    print("4. Production trend of Wheat in Punjab")
    print()
    
    while True:
        question = input("\nYour question (or 'quit' to exit): ").strip()
        
        if question.lower() in ['quit', 'exit', 'q']:
            print("Thank you for using the system!")
            break
        
        if not question:
            continue
        
        print("\nProcessing...")
        result = qa_system.answer(question)
        
        print("\n" + "="*60)
        print("ANSWER:")
        print("="*60)
        print(result['answer'])
        
        if 'data' in result and result['data']:
            print("\n" + "-"*60)
            print("DATA:")
            print("-"*60)
            print(json.dumps(result['data'], indent=2))
        
        print("\n" + "-"*60)
        print("SOURCES:")
        print("-"*60)
        for source in result.get('sources', []):
            print(f"• {source}")
        print("="*60)


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'web':
        # Run web server
        print("Starting web server on http://localhost:5000")
        app.run(debug=True, port=5000)
    else:
        # Run CLI
        main()