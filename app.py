import dash
from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
# import sys
import os
# sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from parse_stats import parse_stats_text
from collections import defaultdict

BASE_PATH = os.getenv("DASH_BASE_PATHNAME","/")
app = dash.Dash(__name__, url_base_pathname=BASE_PATH, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([
    html.H1("SQL Server Statistics Analyzer"),
    html.P("Paste your query statistics below:"),
    dcc.Textarea(
        id='stats-input',
        style={'width': '100%', 'height': 300},
        placeholder='Paste SQL Server statistics here...'
    ),
    html.Br(),
    dbc.Button("Analyze", id='analyze-btn', color='primary', className='mb-3'),
    html.Div(id='results')
], fluid=True)

@app.callback(
    Output('results', 'children'),
    Input('analyze-btn', 'n_clicks'),
    State('stats-input', 'value')
)
def analyze_stats(n_clicks, stats_text):
    if not n_clicks or not stats_text:
        return ""
    try:
        stats = parse_stats_text(stats_text)
        total_logical_reads = 0
        all_queries_content = []
        table_stats = defaultdict(lambda: {'scan_count': 0, 'logical_reads': 0, 'physical_reads': 0})
        for i, query in enumerate(stats, 1):
            query_logical_reads = 0
            query_content = [html.H3(f"Query {i}")]
            for table in query.tables:
                query_content.append(html.Div([
                    html.P(f"Table: {table.table_name}"),
                    html.P(f"  Scan count: {table.scan_count}"),
                    html.P(f"  Logical reads: {table.logical_reads}"),
                    html.P(f"  Physical reads: {table.physical_reads}")
                ]))
                table_stats[table.table_name]['scan_count'] += table.scan_count
                table_stats[table.table_name]['logical_reads'] += table.logical_reads
                table_stats[table.table_name]['physical_reads'] += table.physical_reads
                query_logical_reads += table.logical_reads
            if query.rows_affected:
                query_content.append(html.P(f"Rows affected: {query.rows_affected}"))
            if query.completion_time:
                query_content.append(html.P(f"Completion time: {query.completion_time}"))
            query_content.append(html.P(f"Total logical reads for this query: {query_logical_reads}"))
            total_logical_reads += query_logical_reads
            all_queries_content.append(html.Div(query_content, className='mb-4'))
        all_queries_tab = dbc.Tab(
            html.Div(all_queries_content),
            label="All Queries"
        )
        table_tab = dbc.Tab(
            dbc.Table([
                html.Thead(html.Tr([
                    html.Th("Table Name"),
                    html.Th("Total Scan Count"),
                    html.Th("Total Logical Reads"),
                    html.Th("Total Physical Reads")
                ])),
                html.Tbody([
                    html.Tr([
                        html.Td(table_name),
                        html.Td(stats['scan_count']),
                        html.Td(stats['logical_reads']),
                        html.Td(stats['physical_reads'])
                    ]) for table_name, stats in table_stats.items()
                ])
            ], bordered=True, hover=True, className='mb-4'),
            label="All Tables"
        )
        summary = html.Div([
            html.H2(f"Total logical reads across all queries: {total_logical_reads}"),
            html.Hr()
        ])
        return [summary, dbc.Tabs([all_queries_tab, table_tab])]
    except Exception as e:
        return f"Error parsing statistics: {e}"

if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0', port=8050)
    # app.run(debug=True) 