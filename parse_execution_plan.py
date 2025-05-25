import xml.etree.ElementTree as ET
import pandas as pd
import codecs
import matplotlib.pyplot as plt
import seaborn as sns
from IPython.display import display, HTML
from collections import defaultdict

def parse_execution_plan(content):
    try:
        # Try to decode with UTF-8 first
        try:
            xml_content = content.encode('utf-8').decode('utf-8')
        except UnicodeError:
            # If that fails, try with UTF-8-sig (UTF-8 with BOM)
            try:
                xml_content = content.encode('utf-8-sig').decode('utf-8-sig')
            except UnicodeError:
                raise ValueError("Could not decode the content with UTF-8 encoding")
        
        # Print first few lines for debugging
        print("First 200 characters of XML content:")
        print(xml_content[:200])
        
        # Try to parse with more lenient parser
        try:
            # First try with standard parser
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            print(f"XML Parse Error: {str(e)}")
            print(f"Error location: line {e.position[0]}, column {e.position[1]}")
            
            # Try to show the problematic line
            lines = xml_content.split('\n')
            if e.position[0] <= len(lines):
                print("\nProblematic line:")
                print(lines[e.position[0] - 1])
                print(" " * (e.position[1] - 1) + "^")
            
            # Try to clean the XML content
            # Remove any BOM if present
            if xml_content.startswith('\ufeff'):
                xml_content = xml_content[1:]
            
            # Try parsing again with cleaned content
            try:
                root = ET.fromstring(xml_content)
            except ET.ParseError as e2:
                raise ValueError(f"Could not parse XML after cleaning: {str(e2)}")
        
        # Define the namespace
        ns = {'sp': 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'}
        
        # Initialize lists to store statistics
        stats = []
        statements = []
        
        # First, collect all statements
        for stmt in root.findall('.//sp:StmtSimple', ns):
            stmt_id = stmt.get('StatementId', 'N/A')
            statement = {
                'StatementId': stmt_id,
                'StatementType': stmt.get('StatementType', 'N/A'),
                'StatementText': stmt.get('StatementText', 'N/A')
            }
            statements.append(statement)
            
            for rel_op in stmt.findall('.//sp:RelOp', ns):
                try:
                    # Extract basic statistics
                    step = {
                        'NodeId': rel_op.get('NodeId', 'N/A'),
                        'StatementId': stmt_id,
                        'PhysicalOp': rel_op.get('PhysicalOp', 'N/A'),
                        'LogicalOp': rel_op.get('LogicalOp', 'N/A'),
                        'EstimateRows': rel_op.get('EstimateRows', 'N/A'),
                        'EstimateCPU': rel_op.get('EstimateCPU', 'N/A'),
                        'EstimateIO': rel_op.get('EstimateIO', 'N/A'),
                        'AvgRowSize': rel_op.get('AvgRowSize', 'N/A'),
                        'Parallel': rel_op.get('Parallel', 'N/A'),
                        'EstimatedTotalSubtreeCost': rel_op.get('EstimatedTotalSubtreeCost', 'N/A')
                    }
                    
                    # Add to stats list
                    stats.append(step)
                except Exception as e:
                    print(f"Warning: Error processing RelOp element: {str(e)}")
                    continue
        
        if not stats:
            raise ValueError("No execution plan statistics found in the XML file")
        
        # Convert to DataFrames
        df_stats = pd.DataFrame(stats)
        df_statements = pd.DataFrame(statements)
        
        # Calculate percentages
        try:
            total_cost = float(df_stats['EstimatedTotalSubtreeCost'].iloc[0])
            df_stats['CostPercentage'] = (df_stats['EstimatedTotalSubtreeCost'].astype(float) / total_cost * 100).round(2)
        except (ValueError, TypeError) as e:
            print(f"Warning: Could not calculate cost percentages: {str(e)}")
            df_stats['CostPercentage'] = 0
        
        # Merge with statements
        df_stats = df_stats.merge(df_statements, on='StatementId', how='left')
        
        # Format the output
        df_stats = df_stats[['NodeId', 'StatementId', 'StatementType', 'StatementText', 
                           'PhysicalOp', 'LogicalOp', 'EstimateRows', 'EstimateCPU', 
                           'EstimateIO', 'AvgRowSize', 'Parallel', 'CostPercentage']]
        
        # Rename columns for better readability
        df_stats.columns = ['Node ID', 'Statement ID', 'Statement Type', 'Statement Text',
                          'Physical Operation', 'Logical Operation', 'Estimated Rows',
                          'CPU Cost', 'IO Cost', 'Avg Row Size', 'Parallel', 'Cost %']
        
        return df_stats
    
    except Exception as e:
        raise ValueError(f"Error parsing execution plan: {str(e)}")

# # Parse the execution plan
# try:
#     df = parse_execution_plan('env/sqlexecplanxml.txt')
    
#     # Display the full table
#     display(HTML("<h2>SQL Execution Plan Statistics</h2>"))
#     display(df)
    
#     # Create visualizations
#     plt.figure(figsize=(15, 10))
    
#     # Plot 1: Cost Distribution
#     plt.subplot(2, 2, 1)
#     sns.barplot(data=df.nlargest(10, 'Cost %'), x='Cost %', y='Physical Operation')
#     plt.title('Top 10 Most Expensive Operations')
#     plt.xlabel('Cost Percentage')
    
#     # Plot 2: CPU vs IO Cost
#     plt.subplot(2, 2, 2)
#     plt.scatter(df['CPU Cost'].astype(float), df['IO Cost'].astype(float))
#     plt.title('CPU vs IO Cost Distribution')
#     plt.xlabel('CPU Cost')
#     plt.ylabel('IO Cost')
    
#     # Plot 3: Parallel vs Non-Parallel Operations
#     plt.subplot(2, 2, 3)
#     parallel_counts = df['Parallel'].value_counts()
#     plt.pie(parallel_counts, labels=parallel_counts.index, autopct='%1.1f%%')
#     plt.title('Parallel vs Non-Parallel Operations')
    
#     # Plot 4: Estimated Rows Distribution
#     plt.subplot(2, 2, 4)
#     sns.histplot(data=df, x='Estimated Rows', bins=20)
#     plt.title('Distribution of Estimated Rows')
#     plt.xscale('log')
    
#     plt.tight_layout()
#     plt.show()
    
#     # Print summary statistics
#     print("\nSummary Statistics:")
#     print(f"Total number of execution steps: {len(df)}")
#     print(f"Most expensive operation: {df.loc[df['Cost %'].idxmax(), 'Physical Operation']} ({df['Cost %'].max():.2f}%)")
#     print(f"Total estimated rows: {df['Estimated Rows'].sum():,.0f}")
    
# except Exception as e:
#     print(f"Error: {str(e)}")
#     print("Please check if the file exists and is a valid SQL Server execution plan XML file.")