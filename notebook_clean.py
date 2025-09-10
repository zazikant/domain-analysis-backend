"""
Clean version of the Jupyter notebook without hardcoded API keys
For demonstration purposes - use environment variables in production
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

import re
import json
import requests
import time
from typing import List, Optional
from datetime import datetime

# LangChain imports
from langchain.chains import SequentialChain, LLMChain
from langchain.prompts import PromptTemplate
from langchain.llms.base import LLM
from langchain.schema import BaseOutputParser

# Pydantic for structured outputs
from pydantic import BaseModel, Field

# Google Generative AI
import google.generativeai as genai

# API Configuration - Load from environment variables
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
BRIGHTDATA_API_TOKEN = os.getenv("BRIGHTDATA_API_TOKEN")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

if not all([SERPER_API_KEY, BRIGHTDATA_API_TOKEN, GOOGLE_API_KEY]):
    raise ValueError("Please set all required API keys in your .env file")

# Configure Google AI
genai.configure(api_key=GOOGLE_API_KEY)

print("✅ Setup complete! API keys loaded from environment variables.")
print("🔧 Make sure you have a .env file with your API keys.")
print("📦 Required packages: langchain, google-generativeai, requests, pydantic, python-dotenv")

# Rest of the notebook code would go here...
# (All the classes and functions from the original notebook)