
from pathlib import Path 
import polars as pl

from dash import Dash, html, dcc, Output, Input
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import dash_ag_grid as dag
 
import plotly.graph_objects as go


file_path = Path(f"C:/*/*.parquet")
df_stores = pl.scan_parquet(file_path).collect()

artIds = pl.scan_parquet(f"C:/*/*/*.parquet",
                        hive_partitioning=True).select(pl.col("custArtId")).unique().collect().sort("custArtId")["custArtId"].to_list()

app = Dash(external_stylesheets = [dbc.themes.BOOTSTRAP])
app.title = "ETOS Dashboard"

server = app.server

row1 = html.Div([dbc.Row([dbc.Col([dbc.Row("store:"),
                        dcc.Dropdown(id="my-Store", options=df_stores.columns)], width={"order": "first"} ),
                        dbc.Col([dbc.Row("ArtId:"),
                                 dcc.Dropdown(id="my-ArtId")] ), 
                                                  
                        ], style={"width": "50%"})
])

row2 = html.Div([
    dbc.Col([
        dbc.Row("Filter:"),
        dcc.RadioItems(
            id='radio-selector',
            options=[
                {'label': 'Disable dropdown', 'value': 'off'},
                {'label': 'Enable dropdown', 'value': 'on'}
            ],
            value='off',
            inline=True,
            labelStyle={"margin": ".5rem"}
        ),
        dbc.Row("ArtSize:"),
        dcc.Dropdown(id="my-ArtSize")
    ])
], style={"width": "50%"})

graph_1 = dbc.Card(dcc.Graph(figure=go.Figure(), id='my-Fig'))
graph_2 = dbc.Card(dcc.Graph(figure=go.Figure(), id='my-Fig2'))

row3 = html.Div([dbc.Row([
                        dbc.Col([dbc.Row("ArtId:"),
                                 dcc.Dropdown(id="my-ArtId-bar", options=artIds, value=artIds[0])],
                                 ), 
                        ], style={"width": "50%"})
])

## App layout
app.layout = dbc.Container([ html.H1("Dashboard", className="text", style={'textAlign': 'center'}),
                            html.H3("test", className="text", style={'textAlign': 'center'}),
                            html.Br(),
                            dcc.Tabs([dcc.Tab(label="Stock Overview",
                                       children=[ html.Br(),                                                      
                                                    html.Br(),                                                                                             
                                                    row1,
                                                    html.Br(),
                                                    row2,
                                                    html.Br(),                                                    
                                                    html.Br(),
                                                    graph_1 ,
                                                    html.Br(),
                                                    row3,
                                                    graph_2                                                                        
                                        ], style={'padding': '0','line-height': 35}, selected_style={'padding': '0','line-height': 35}),                                                                                    
                                ], style={'width': '70%', 'font-size': '90%','height':40}) 
                            ]) 
    
# Store callback to pick desired store number
@app.callback(             
    Output(component_id='my-ArtId', component_property='options'),       
    Input(component_id='my-Store', component_property='value'))
def article_picker(store):
    if store:        
        file_path = Path(f"C:/*/*/*/*.parquet")        
        df = pl.scan_parquet(file_path).collect()
        # print(df)
        return sorted(df["custArtId"].unique().to_list())
    else:
        raise PreventUpdate    
    
# SizeId callback to pick article Size
@app.callback(             
    Output(component_id='my-ArtSize', component_property='options'),         
    Input(component_id='my-Store', component_property='value'),
    Input(component_id='my-ArtId', component_property='value'))
def article_size_picker(store, artId):
    if store and artId:   
        file_path = Path(f"C:/*/*.parquet")
        df = pl.scan_parquet(file_path).filter(pl.col("custArtId").eq(int(artId))).collect()                                          
        return sorted(df["custSizeId"].unique().to_list())
    else:
        raise PreventUpdate 
    
# Callback to enable/disable dropdown
@app.callback(
    Output('my-ArtSize', 'disabled'),
    Input('radio-selector', 'value')
)
def toggle_dropdown(radio_value):
    if radio_value == 'on':
        return False  # enable dropdown
    return True       # disable dropdown
    
# Callback to plot weekly sales
@app.callback(        
    Output(component_id='my-Fig', component_property='figure'),      
    Input(component_id='my-Store', component_property='value'),
    Input(component_id='my-ArtId', component_property='value'),
    Input(component_id='radio-selector', component_property='value'),
    Input(component_id='my-ArtSize', component_property='value')
)
def plot(store, artId, active, artSize):
    fig_go = go.Figure()     
    if  store and artId :
        file_path = Path(f"C:/*/*.parquet")             
        if active == 'off':
            # Read data and trransform date into calender week
            df = (pl.scan_parquet(file_path,
                                hive_partitioning=True).filter(
                                                        pl.col("quantity") >= 0,
                                                        pl.col("custArtId").eq(int(artId)))
                                                        .with_columns(pl.col("bookingDate").cast(pl.String)
                                                                                        .str.strptime(pl.Date, format="%Y%m%d"),
                                                                    pl.col("bookingDate").cast(pl.String).str.to_date(format="%Y%m%d").dt.week().alias('calender_week'))
                                                        .collect()).drop(["bookingDate", "customerId"])                         

            # Aggregate sales data by calendar week
            for year in df.get_column("year").unique().to_list():
                df_year = df.filter(pl.col("year") == year).group_by(["custArtId", "calender_week", "year", "custStoreId"]).agg(pl.col("quantity").sum()).sort(["calender_week"])
                # Plot weekly sales given the year 
                # print(df_year)
                fig_go.add_trace(go.Scatter(x=df_year.get_column("calender_week").to_list(), 
                                            y=df_year.get_column("quantity").to_list(),
                                            mode='markers',name=str(year),
                                            # hovertext=df_year.get_column("custSizeId").to_list( ),
                                            # hoverinfo='text')
                                            ))      
                  
                fig_go.update_traces(showlegend=True)
                fig_go.update_layout(
                title={
                'text': f"Historical sales data for article {artId} at store {store}",
                'y':0.95,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
                xaxis=dict(            
                            title="calender_week",
                            dtick=1,
                            tick0=1),
                            yaxis_title="Sales",
                            legend_title_text='year')        
            return fig_go        
        
        else:  # active == 'on'
            if  store and artId and artSize:
                # Read data and trransform date into calender week
                df = (pl.scan_parquet(file_path,
                                    hive_partitioning=True).filter(
                                                            pl.col("quantity") >= 0,
                                                            pl.col("custArtId").eq(int(artId)),
                                                            pl.col("custSizeId").eq(int(artSize)))
                                                            .with_columns(pl.col("bookingDate").cast(pl.String)
                                                                                            .str.strptime(pl.Date, format="%Y%m%d"),
                                                                        pl.col("bookingDate").cast(pl.String).str.to_date(format="%Y%m%d").dt.week().alias('calender_week'))
                                                            .collect()).drop(["bookingDate", "customerId"])  
                # Aggregate sales data by calendar week
                for year in df.get_column("year").unique().to_list():
                    df_year = df.filter(pl.col("year") == year).group_by(["custArtId", "custSizeId", "calender_week", "year", "custStoreId"]).agg(pl.sum("quantity")).sort(["calender_week"])
                    # print(df_year)
                    fig_go.add_trace(go.Scatter(x=df_year.get_column("calender_week").to_list(), 
                                                y=df_year.get_column("quantity").to_list(),
                                                mode='markers',name=str(year)))                 
                    fig_go.update_traces(showlegend=True)
                    fig_go.update_layout(
                    title={
                    'text': f"Historical sales of article {artId} in store {store}",
                    'y':0.95,
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'},
                    xaxis=dict(
                            #tickformat="%Y-%m-%d",  # Format without time
                            title="calender_week",
                            dtick=1,
                            tick0=1),
                            yaxis_title="Sales",
                            legend_title_text='year')        
                return fig_go
            else:
                raise PreventUpdate  
    

# Callback to plot weekly sales per year
@app.callback(         
    Output(component_id='my-Fig2', component_property='figure'), 
    Input(component_id='my-ArtId-bar', component_property='value') 
)
def plot_bar(artId):
    
    if artId :
        f1 = Path(f"C:/*/*/*/*.parquet")
        fig_go2 = go.Figure()
        store_name = list(map(int, df_stores.columns))  
        store_id = list(map(int, df_stores.row(0)))
        # read and Aggregate sales data by calendar week
        df2 = (pl.scan_parquet(f1,
                            hive_partitioning=True).filter(pl.col("custStoreId").is_in(store_name),
                                                    pl.col("quantity") >= 0,
                                                    pl.col("custArtId").eq(int(artId)))
                                                    .with_columns(pl.col("bookingDate").cast(pl.String)
                                                                                    .str.strptime(pl.Date, format="%Y%m%d"),
                                                                pl.col("bookingDate").cast(pl.String).str.to_date(format="%Y%m%d").dt.week().alias('calender_week'))
                                                    .drop(["bookingDate", "customerId"])
                                                    .group_by(["custArtId", "calender_week", "year", "custStoreId"]).agg(pl.col("quantity").sum()).sort(["calender_week"])
                                                    .collect())
        
        df2 = df2.join(df_stores.transpose(include_header=True).with_columns(pl.col("column").cast(pl.Int64)).rename({"column_0": "store_number"}),
                        left_on="custStoreId", right_on="column", how='left')

        for year in df2.get_column("year").unique().to_list():
            df2_year = df2.filter(pl.col("year") == year)           
            fig_go2.add_trace(go.Bar(x=df2_year.get_column("store_number").cast(pl.Int64).to_list(),
                                    y=df2_year.get_column("quantity").to_list(),
                                    name=str(year),                                            
                                            ))
            fig_go2.update_traces(showlegend=True)
            fig_go2.update_layout(
            title={
            'text': f"Historical sales data for article {artId} across all stores",
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
            xaxis=dict(
            
                title="store number",
                tickmode='array',
                tickvals=store_id,
                ),
                yaxis_title="Sales count",
                legend_title_text='year')

            fig_go2.update_xaxes(showticklabels=True)
        return fig_go2
    else:
        raise PreventUpdate


# Run the app
if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=8050)