import xml.etree.ElementTree as ET
import pandas as pd
import codecs
import matplotlib.pyplot as plt
import seaborn as sns
from IPython.display import display, HTML

def parse_execution_plan(xml_file):
    try:
        # Read the file in binary mode first
        with open(xml_file, 'rb') as f:
            content = f.read()
            
        # Try to decode with UTF-16 without BOM
        try:
            xml_content = content.decode('utf-16-le')
            print("Successfully read file with UTF-16-LE encoding")
        except UnicodeDecodeError:
            try:
                xml_content = content.decode('utf-16-be')
                print("Successfully read file with UTF-16-BE encoding")
            except UnicodeDecodeError:
                # Fallback to UTF-8
                xml_content = content.decode('utf-8')
                print("Successfully read file with UTF-8 encoding")
        
        # Parse the XML content
        root = ET.fromstring(xml_content)
        
        # Define the namespace
        ns = {'sp': 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'}
        
        # Initialize lists to store statistics
        stats = []
        
        # Find all RelOp elements (these contain the execution plan steps)
        for rel_op in root.findall('.//sp:RelOp', ns):
            try:
                # Extract basic statistics
                step = {
                    'NodeId': rel_op.get('NodeId', 'N/A'),
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
        
        # Convert to DataFrame
        df = pd.DataFrame(stats)
        
        # Calculate percentages
        try:
            total_cost = float(df['EstimatedTotalSubtreeCost'].iloc[0])
            df['CostPercentage'] = (df['EstimatedTotalSubtreeCost'].astype(float) / total_cost * 100).round(2)
        except (ValueError, TypeError) as e:
            print(f"Warning: Could not calculate cost percentages: {str(e)}")
            df['CostPercentage'] = 0
        
        # Format the output
        df = df[['NodeId', 'PhysicalOp', 'LogicalOp', 'EstimateRows', 'EstimateCPU', 
                 'EstimateIO', 'AvgRowSize', 'Parallel', 'CostPercentage']]
        
        # Rename columns for better readability
        df.columns = ['Node ID', 'Physical Operation', 'Logical Operation', 'Estimated Rows',
                     'CPU Cost', 'IO Cost', 'Avg Row Size', 'Parallel', 'Cost %']
        
        return df
    
    except Exception as e:
        print(f"Error parsing execution plan: {str(e)}")
        raise

# Parse the execution plan
try:
    df = parse_execution_plan('env/sqlexecplanxml.txt')
    
    # Display the full table
    display(HTML("<h2>SQL Execution Plan Statistics</h2>"))
    display(df)
    
    # Create visualizations
    plt.figure(figsize=(15, 10))
    
    # Plot 1: Cost Distribution
    plt.subplot(2, 2, 1)
    sns.barplot(data=df.nlargest(10, 'Cost %'), x='Cost %', y='Physical Operation')
    plt.title('Top 10 Most Expensive Operations')
    plt.xlabel('Cost Percentage')
    
    # Plot 2: CPU vs IO Cost
    plt.subplot(2, 2, 2)
    plt.scatter(df['CPU Cost'].astype(float), df['IO Cost'].astype(float))
    plt.title('CPU vs IO Cost Distribution')
    plt.xlabel('CPU Cost')
    plt.ylabel('IO Cost')
    
    # Plot 3: Parallel vs Non-Parallel Operations
    plt.subplot(2, 2, 3)
    parallel_counts = df['Parallel'].value_counts()
    plt.pie(parallel_counts, labels=parallel_counts.index, autopct='%1.1f%%')
    plt.title('Parallel vs Non-Parallel Operations')
    
    # Plot 4: Estimated Rows Distribution
    plt.subplot(2, 2, 4)
    sns.histplot(data=df, x='Estimated Rows', bins=20)
    plt.title('Distribution of Estimated Rows')
    plt.xscale('log')
    
    plt.tight_layout()
    plt.show()
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"Total number of execution steps: {len(df)}")
    print(f"Most expensive operation: {df.loc[df['Cost %'].idxmax(), 'Physical Operation']} ({df['Cost %'].max():.2f}%)")
    print(f"Total estimated rows: {df['Estimated Rows'].sum():,.0f}")
    
except Exception as e:
    print(f"Error: {str(e)}")
    print("Please check if the file exists and is a valid SQL Server execution plan XML file.")