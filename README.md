# Climate Report Generator 🌍📈

An automated report generation system built with **Flask** and **Google Gemini AI**. This tool streamlines the creation of comprehensive climate risk assessment reports by aggregating data from multiple sources, generating AI-powered narratives, and outputting professional Word documents.

## 🚀 Features

- **AI-Powered Narratives**: Integrates with **Google Gemini** to analyze site context and climate hazards, generating bespoke executive summaries and technical sections.
- **Dynamic Word Generation**: Uses `python-docx` to populate pre-defined Word templates with dynamic text, tables, and images.
- **Workflow Integrations**:
  - **Mural**: Fetches and processes workshop data for inclusion in reports.
  - **Excel**: Automatically parses and formats complex data tables from Excel workbooks.
  - **Dropbox**: Supports fetching assets from and uploading final reports to Dropbox.
- **Real-time Progress**: Integrated **Socket.IO** provides live updates on the report generation status.
- **Smart Caching**: Implements a local cache for AI responses to optimize speed and reduce API costs.
- **Data Visualization**: Generates climate-relevant charts and visuals using `matplotlib`.

## 🛠️ Tech Stack

- **Backend**: Python / Flask
- **Real-time**: Flask-SocketIO / Gevent
- **AI/LLM**: Google Generative AI (Gemini)
- **Document Processing**: `python-docx`, `openpyxl`
- **Visualization**: `matplotlib`
- **Storage**: Dropbox SDK
- **Environment**: `python-dotenv`

## 📋 Prerequisites

- Python 3.9+
- Google Gemini API Key
- (Optional) Dropbox App Credentials
- (Optional) Mural API Credentials

## ⚙️ Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/DigiCareHouse/Climate-Report-Angie-
   cd flask-gemini-report
   ```

2. **Create a virtual environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**:
   Create a `.env` file in the root directory:
   ```env
   GEMINI_API_KEY=your_gemini_key
   DROPBOX_TOKEN=your_dropbox_token
   DROPBOX_APP_KEY=your_app_key
   DROPBOX_APP_SECRET=your_app_secret
   DROPBOX_REFRESH_TOKEN=your_refresh_token
   ```

## 🏃 Usage

1. Start the Flask application:
   ```bash
   python app.py
   ```
2. Open your browser and navigate to `http://127.0.0.1:5000`.
3. Upload your data files (JSON, Excel, Images).
4. Configure your report parameters and click **Generate**.
5. Track the progress in real-time and download the final `.docx` report.

## 📂 Project Structure

- `app.py`: Main application logic and report generation engine.
- `mural_integration.py`: Logic for Mural workshop data extraction.
- `temp_styles.css`: Custom UI styling.
- `templates/`: HTML templates for the web interface.
- `static/`: Frontend assets (JS, CSS, Images).
- `config/`: Configuration files and report settings.
- `output/`: Generated reports are stored here.
- `uploaded/`: Temporary storage for user-uploaded assets.

## 📄 License

This project is proprietary. All rights reserved.
