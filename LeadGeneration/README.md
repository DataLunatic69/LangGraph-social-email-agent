# Email Automation System

An intelligent email management system that:
1. Sends personalized initial emails to leads from CSV files
2. Monitors your inbox for responses
3. Classifies incoming emails
4. Automatically responds to personal emails
5. Flags and categorizes automated messages

## Features

- **Lead Outreach**: Send personalized emails to marketing leads from CSV files
- **Smart Classification**: AI-powered email categorization (personal vs automated)
- **Auto-Response**: Generate context-aware replies to personal emails
- **Label Management**: Automatically flag and categorize processed emails
- **Continuous Operation**: Runs in the background with configurable check intervals

## Prerequisites

- Python 3.8+
- Google Cloud Platform account
- Ollama with Llama3 model running locally

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/email-automation.git
   cd email-automation```

2. Set up virtual environment

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```
3. Install dependencies:
```bash
   pip install -r requirements.txt
```

## Configuration:
## Google Cloud Setup:

```bash
Create a project in Google Cloud Console

Enable Gmail API

Create OAuth 2.0 credentials (Desktop App type)

Download credentials.json to the project root
```

## CSV Files:

```bash
Place your lead files (ziellabs_leads.csv or global_marketing_leads.csv) in the parent directory
```

## Supported formats:

```bash
company,website,emails,status,...  # ziellabs format
email,name,company,...             # global marketing format

```
