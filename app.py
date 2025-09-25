from pymongo import MongoClient
import os, re
from statistics import mean

def get_page_count(doc, pattern):
    """Extract page count from document OCR markdown"""
    markdown = doc.get('ocr', {}).get('markdown')
    return len(pattern.findall(markdown)) if markdown else 0

def get_team_data(teams_collection, team_name):
    """Get team data from teams collection"""
    try:
        total_coders = teams_collection.count_documents({})
        team_doc = teams_collection.find_one({"name": team_name})
        team_members = team_doc.get('team_members', []) if team_doc else []
        return total_coders, len(team_members), team_members
    except Exception as e:
        print(f"Error getting team data: {e}")
        return 0, 0, []

def split_charts_optimally(charts, team_members, pattern):
    """Split charts using Greedy Load Balancing"""
    member_names = [m['name'] for m in team_members if isinstance(m, dict) and m.get('name')]
    if not member_names:
        return {}
    
    # Sort charts by pages (descending) and initialize workloads
    charts_sorted = sorted(charts, key=lambda x: get_page_count(x, pattern), reverse=True)
    workloads = {name: {'charts': [], 'total_pages': 0} for name in member_names}
    
    # Assign each chart to member with minimum current load
    for chart in charts_sorted:
        pages = get_page_count(chart, pattern)
        min_member = min(workloads.keys(), key=lambda x: workloads[x]['total_pages'])
        workloads[min_member]['charts'].append({
            'id': str(chart.get('_id')), 'filename': chart.get('filename', 'N/A'), 'pages': pages
        })
        workloads[min_member]['total_pages'] += pages
    
    return workloads

def write_assignment_report(assignments, output_file):
    """Write assignment report"""
    try:
        total_charts = sum(len(data['charts']) for data in assignments.values())
        total_pages = sum(data['total_pages'] for data in assignments.values())
        loads = [data['total_pages'] for data in assignments.values()]
        min_load, max_load = min(loads), max(loads)
        
        with open(output_file, 'a') as f:
            f.write(f"\n{'='*60}\nCHART ASSIGNMENT REPORT\n{'='*60}\n\n"
                   f"Summary: {total_charts} charts, {total_pages} pages, {len(assignments)} members\n"
                   f"Average: {total_pages/len(assignments):.2f} pages/member\n\n")
            f.write(f"split the charts using Greedy Load Balancing\n\n")

            for member, data in assignments.items():
                f.write(f"{member}: {len(data['charts'])} charts, {data['total_pages']} pages\n")
                for i, chart in enumerate(data['charts'], 1):
                    f.write(f"  {i}. {chart['filename']} ({chart['pages']} pages)\n")
                f.write(f"{'-'*40}\n")
            
            f.write(f"\nLoad Balance: Min={min_load}, Max={max_load}, Variance={max_load-min_load}\n"
                   f"Efficiency: {(1-(max_load-min_load)/max_load)*100 if max_load > 0 else 100:.1f}%\n")
    except Exception as e:
        print(f"Error writing report: {e}")

def main():
    client = MongoClient('mongodb://localhost:27017/')  # Update with your MongoDB URI
    db = client['']  # Database name
    client_collection = db[''] # Client Collection
    teams_collection = db[''] # Teams collection
    query = {
        "workflow.action_owner": "system", "workflow.current_action": "unassigned", 
        "workflow.current_queue": "pool", "ocr.markdown": {"$exists": True, "$ne": "", "$ne": None}
    }
    
    os.makedirs("tmp", exist_ok=True)
    output_file = "tmp/charts_output.txt"
    pattern = re.compile(r'<!--\s*page\s+(\d+)\s*-->', re.IGNORECASE)
    
    try:
        # Get data
        team_name = 'TeamA'
        total_coders, team_member_count, team_members = get_team_data(teams_collection, team_name)
        initial_results = list(client_collection.find(query))
        results = [doc for doc in initial_results if get_page_count(doc, pattern) > 0]
        
        # Write results
        with open(output_file, 'w') as f:
            f.write(f"Teams: {total_coders}\nTeamA Members: {team_member_count}\n"
                   f"Members: {', '.join(m.get('name', '') for m in team_members) if team_members else 'None'}\n{'='*60}\n\n")
            
            for i, doc in enumerate(results, 1):
                workflow = doc.get('workflow', {})
                f.write(f"Document {i}:\nID: {doc.get('_id')}\nFile: {doc.get('filename', 'N/A')}\n"
                       f"Pages: {get_page_count(doc, pattern)}\n{'-'*50}\n")
            
            f.write(f"\nFiltered Results: {len(results)} documents\n")
        
        if team_members and results:
            print(f"Processing: {len(initial_results)} → {len(results)} documents with pages")
            assignments = split_charts_optimally(results, team_members, pattern)
            write_assignment_report(assignments, output_file)
            
            print("\nAssignments:")
            for member, data in assignments.items():
                print(f"{member}: {len(data['charts'])} charts, {data['total_pages']} pages")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    main()