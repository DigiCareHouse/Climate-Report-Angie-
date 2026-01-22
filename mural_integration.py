# mural_integration.py
import os
import json
import pandas as pd
from datetime import datetime
from openpyxl import Workbook
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()


class MuralDataExtractor:
    def __init__(self):
        self.access_token = os.environ.get("MURAL_ACCESS_TOKEN")
        self.base_url = "https://app.mural.co/api/public/v1"
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }
        self.output_folder = "mural_data"
        os.makedirs(self.output_folder, exist_ok=True)

    def test_connection(self):
        """Test Mural API connection"""
        print("🔗 Testing Mural API connection...")
        try:
            response = requests.get(f"{self.base_url}/identity", headers=self.headers)
            if response.status_code == 200:
                user_data = response.json()
                print(f"✅ Connected as: {user_data.get('name', 'Unknown')}")
                print(f"📧 Email: {user_data.get('email', 'Unknown')}")
                return True
            else:
                print(f"❌ Connection failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Connection error: {e}")
            return False

    def get_all_workspaces(self):
        """Get all workspaces"""
        print("📁 Fetching workspaces...")
        response = requests.get(f"{self.base_url}/workspaces", headers=self.headers)
        if response.status_code == 200:
            workspaces = response.json()
            print(f"✅ Found {len(workspaces)} workspace(s)")
            return workspaces
        else:
            print(f"❌ Failed to get workspaces: {response.status_code}")
            return []

    def get_rooms_for_workspace(self, workspace_id):
        """Get rooms for a specific workspace"""
        print(f"🏢 Fetching rooms for workspace {workspace_id}...")
        response = requests.get(f"{self.base_url}/workspaces/{workspace_id}/rooms", headers=self.headers)
        if response.status_code == 200:
            rooms = response.json()
            print(f"✅ Found {len(rooms)} room(s)")
            return rooms
        else:
            print(f"❌ Failed to get rooms: {response.status_code}")
            return []

    def get_murals_for_room(self, room_id):
        """Get murals for a specific room"""
        print(f"🎨 Fetching murals for room {room_id}...")
        response = requests.get(f"{self.base_url}/rooms/{room_id}/murals", headers=self.headers)
        if response.status_code == 200:
            murals = response.json()
            print(f"✅ Found {len(murals)} mural(s)")
            return murals
        else:
            print(f"❌ Failed to get murals: {response.status_code}")
            return []

    def get_mural_content(self, mural_id):
        """Get detailed content of a specific mural"""
        print(f"📄 Fetching content for mural {mural_id}...")
        response = requests.get(f"{self.base_url}/murals/{mural_id}", headers=self.headers)
        if response.status_code == 200:
            mural_data = response.json()
            return mural_data
        else:
            print(f"❌ Failed to get mural content: {response.status_code}")
            return None

    def get_widgets_from_mural(self, mural_id):
        """Get widgets/objects from a mural"""
        print(f"🧱 Fetching widgets for mural {mural_id}...")
        response = requests.get(f"{self.base_url}/murals/{mural_id}/widgets", headers=self.headers)
        if response.status_code == 200:
            widgets = response.json()
            print(f"✅ Found {len(widgets)} widget(s)")
            return widgets
        else:
            print(f"❌ Failed to get widgets: {response.status_code}")
            return []

    def extract_table_data(self, widgets, table_name):
        """Extract table data from widgets based on table name pattern"""
        table_data = []

        # Find text widgets that might contain table data
        for widget in widgets:
            if widget.get('type') in ['text', 'sticky_note']:
                content = widget.get('text', '').strip()
                if content:
                    # Check if this looks like table data
                    if table_name.lower() in content.lower():
                        # Try to parse table rows
                        lines = content.split('\n')
                        for line in lines:
                            # Split by tabs or multiple spaces
                            if '\t' in line:
                                row = [cell.strip() for cell in line.split('\t')]
                            elif '  ' in line:
                                row = [cell.strip() for cell in line.split('  ') if cell.strip()]
                            elif '|' in line:
                                row = [cell.strip() for cell in line.split('|') if cell.strip()]
                            else:
                                row = [line.strip()]

                            if row and any(row):
                                table_data.append(row)

        return table_data

    def export_to_excel(self, data_dict, filename_prefix="mural_data"):
        """Export data to Excel file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.xlsx"
        filepath = os.path.join(self.output_folder, filename)

        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            for sheet_name, data in data_dict.items():
                if data:
                    # Convert to DataFrame
                    if isinstance(data, list):
                        if all(isinstance(row, list) for row in data):
                            # List of lists - table data
                            df = pd.DataFrame(data)
                        else:
                            # List of dictionaries
                            df = pd.DataFrame(data)
                    elif isinstance(data, dict):
                        # Single dictionary
                        df = pd.DataFrame([data])
                    else:
                        continue

                    # Write to Excel sheet
                    df.to_excel(writer, sheet_name=sheet_name[:31], index=False)  # Sheet name max 31 chars

        print(f"💾 Data exported to: {filepath}")
        return filepath

    def extract_climate_tables(self):
        """Extract climate-related tables from Mural"""
        print("🌍 Looking for climate adaptation tables in Mural...")

        all_tables = {}

        # Get workspaces
        workspaces = self.get_all_workspaces()

        for workspace in workspaces[:2]:  # Limit to first 2 workspaces
            workspace_id = workspace.get('id')
            workspace_name = workspace.get('name', f'Workspace_{workspace_id[:8]}')

            # Get rooms
            rooms = self.get_rooms_for_workspace(workspace_id)

            for room in rooms[:3]:  # Limit to first 3 rooms
                room_id = room.get('id')
                room_name = room.get('name', f'Room_{room_id[:8]}')

                # Get murals
                murals = self.get_murals_for_room(room_id)

                for mural in murals[:5]:  # Limit to first 5 murals
                    mural_id = mural.get('id')
                    mural_title = mural.get('title', f'Mural_{mural_id[:8]}')

                    print(f"🔍 Analyzing: {mural_title}")

                    # Look for climate-related murals
                    climate_keywords = [
                        'climate', 'adaptation', 'risk', 'vulnerability',
                        'impact', 'assessment', 'table', 'data',
                        'hot summer', 'tropical nights', 'flood',
                        'drought', 'wind', 'subsidence', 'hazards',
                        'monitoring', 'capacity', 'actions'
                    ]

                    if any(keyword in mural_title.lower() for keyword in climate_keywords):
                        print(f"✅ Found climate-related mural: {mural_title}")

                        # Get widgets from mural
                        widgets = self.get_widgets_from_mural(mural_id)

                        # Extract specific tables
                        table_patterns = {
                            'table-1_identified-impacts': ['identified impacts', 'table 1', 'impacts requiring action'],
                            'table-3_a': ['physical risk', 'table 3', 'risk management', 'adaptation actions'],
                            'table-4_current_strengths': ['current strengths', 'table 4', 'adaptive capacity'],
                            'table-5_development_actions': ['capacity development', 'table 5', 'development actions'],
                            'table-7_monitoring': ['monitoring', 'table 6', 'table 7', 'review processes'],
                            'table-A2_hazards': ['hazards', 'table a2', 'climate hazards', 'ea hazards'],
                            'table_A5_monitoring': ['monitoring matrix', 'table a5', 'evaluation matrix'],
                            'cadd-1_current': ['cadd', 'current capabilities', 'capacity diagnosis'],
                            'cadd-2_add': ['additional capabilities', 'cadd add'],
                            'rapa-1': ['rapa', 'rapid adaptation', 'adaptation pathways'],
                            'rapa-2': ['rapa 2', 'adaptation pathways assessment']
                        }

                        for table_key, keywords in table_patterns.items():
                            table_data = self.extract_table_data(widgets, table_key)
                            if table_data:
                                print(f"📊 Extracted {len(table_data)} rows for {table_key}")
                                all_tables[table_key] = table_data

                        # Also get the full mural content for analysis
                        mural_content = self.get_mural_content(mural_id)
                        if mural_content:
                            content_key = f"mural_{mural_id[:8]}"
                            all_tables[content_key] = [['Title', mural_title],
                                                       ['Created', mural_content.get('createdAt', '')],
                                                       ['Updated', mural_content.get('updatedAt', '')],
                                                       ['Description', mural_content.get('description', '')]]

        return all_tables

    def generate_excel_for_report(self):
        """Generate Excel file with extracted Mural data for report generation"""
        print("🚀 Starting Mural data extraction for report generation...")

        # Test connection first
        if not self.test_connection():
            print("❌ Cannot connect to Mural API. Please check your access token.")
            return None

        # Extract climate tables
        all_tables = self.extract_climate_tables()

        if not all_tables:
            print("⚠️ No climate tables found in Mural. Creating template Excel.")
            # Create template tables based on your report structure
            all_tables = self.create_template_tables()

        # Export to Excel
        excel_file = self.export_to_excel(all_tables, "climate_tables")

        # Also save as JSON for reference
        json_file = os.path.join(self.output_folder, f"mural_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(all_tables, f, indent=2, ensure_ascii=False)
        print(f"💾 JSON data saved to: {json_file}")

        return excel_file

    def create_template_tables(self):
        """Create template tables based on report structure"""
        print("📝 Creating template tables...")

        templates = {
            'table-1_identified-impacts': [
                ['Global Warming Level (°C)', 'Key Climate Change Impacts Requiring Action'],
                ['0.5°C', 'The earliest, noticeable impacts have arrived'],
                ['', '- Reduced livestock productivity due to heat stress'],
                ['', '- Increased frequency of surface water flooding'],
                ['1.0°C', 'Impacts become chronic and more severe'],
                ['', '- More frequent transport disruptions due to flooded roads'],
                ['', '- Increased operational costs for cooling and water management'],
                ['1.5°C', 'Chronic, compounding problems occur'],
                ['', '- Significant heat stress reducing milk yields by 10-15%'],
                ['', '- Water scarcity affecting pasture growth'],
                ['2.0°C', 'Catastrophic risks emerging'],
                ['', '- Critical infrastructure at risk from extreme flooding'],
                ['', '- Supply chain disruptions affecting feed availability'],
                ['3.0°C+', 'Near-irreversible loss and existential threats'],
                ['', '- Farm viability threatened by compounding climate impacts'],
                ['', '- Need for transformational change or relocation']
            ],

            'table-3_a': [
                ['Hazards', 'Adaptation Actions', 'Decision Triggers', 'Comments'],
                ['Heat Stress', 'Install additional shading and ventilation', '3 consecutive days >25°C',
                 'Priority for dairy herd'],
                ['', 'Provide supplementary cooling systems', 'Heat stress index >28°C',
                 'Consider for high-value livestock'],
                ['Flooding', 'Raise critical equipment above flood level', 'Flood warning issued',
                 'Immediate action required'],
                ['', 'Install surface water drainage improvements', '2 flooding events in one season',
                 'Long-term infrastructure'],
                ['Drought', 'Implement water harvesting and storage', '30 days without significant rain',
                 'Essential for summer months'],
                ['', 'Drought-resistant pasture species', 'Soil moisture <40% capacity', 'Gradual implementation']
            ],

            'table-4_current_strengths': [
                ['Current Adaptive Capacity Strengths'],
                ['Experienced farm management team with local knowledge'],
                ['Established relationships with agricultural suppliers'],
                ['Existing infrastructure that can be adapted'],
                ['Strong community network for mutual support'],
                ['Basic monitoring systems already in place']
            ],

            'table-5_development_actions': [
                ['Capacity Development Action', 'Decision Trigger', 'Type & Lead', 'Timing'],
                ['Short term', '', '', ''],
                ['Senior leadership discuss climate change impact', '', '', ''],
                ['Stimulate discussion of climate impacts to address all significant implications', '', '', ''],
                ['Maintain ongoing discussion of climate impacts', 'Quarterly board meeting', 'CEO & Board', 'Ongoing'],
                ['Review what additional leadership support is required', 'Annual review', 'Management Team', 'Year 1'],
                ['Amend discussion and support as appropriate', 'Emerging risks identified', 'CEO', 'As needed']
            ],

            'table-7_monitoring': [
                ['Process', 'Frequency', 'Responsible Party'],
                ['Climate data review', 'Monthly', 'Farm Manager'],
                ['Risk assessment update', 'Quarterly', 'Climate Committee'],
                ['Adaptation progress review', 'Bi-annually', 'Board of Directors'],
                ['Full plan review', 'Annually', 'CEO & Board']
            ],

            'table-A2_hazards': [
                ['Environment Agency', 'EA/Gov.UK', 'Met Office', 'MunichRe', 'Bespoke enquiry'],
                ['Fluvial flooding', 'Yes', 'Yes', 'Yes', 'Yes'],
                ['Surface water flooding', 'Yes', 'Yes', 'Yes', 'Yes'],
                ['Coastal flooding', 'Yes', 'NA', 'Yes', 'NA'],
                ['Heatwave', 'Yes', 'Yes', 'Yes', 'Yes'],
                ['Drought', 'Yes', 'Yes', 'Yes', 'Yes'],
                ['Strong winds', 'Yes', 'Yes', 'Yes', 'Yes'],
                ['Subsidence', 'NA', 'Yes', 'Yes', 'NA']
            ],

            'table_A5_monitoring': [
                ['Indicator', 'Baseline', 'Target', 'Current', 'Gap', 'Actions'],
                ['Heat stress days', '5 days/year', '≤3 days/year', '7 days', '+2 days', 'Install cooling'],
                ['Flood events', '2 events/year', '≤1 event/year', '3 events', '+1 event', 'Drainage work'],
                ['Water availability', '60 days reserve', '90 days reserve', '45 days', '-15 days', 'Harvesting'],
                ['Pasture quality', 'Score: 7/10', 'Score: 8/10', '6/10', '-1 point', 'Species change']
            ]
        }

        return templates