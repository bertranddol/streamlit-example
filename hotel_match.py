import pydeck as pdk
import pandas as pd
import streamlit as st
import numpy as np
import snowflake.connector
#import sys
import warnings
warnings.filterwarnings('ignore')

@st.experimental_singleton
def init_snowflake_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])

@st.cache
def run_snowflake_query(query):
    with st.session_state.conn.cursor() as cur:
        try:
            cur.execute(query)
            return cur.fetchall()
        except Exception as ee:
            #print (f"Query error={ee}")
            return []

@st.cache
def get_page_number(page, total_nb_match, one):
    page += one
    if page + 1 > total_nb_match:
        page = 0
    if page < 0:
        page = total_nb_match - 1
    return page

@st.cache
def get_max_date():
    query = f"SELECT max(ql2_day) \
            from QL2_HOTELMATCH.PUBLIC.DAILY_GEOBOX_MATCH ;"
    rows = run_snowflake_query( query )
    ql2_day = rows[0][0]
    return ql2_day

#@st.cache
def get_hotel_data(ql2_day):
    query = f"SELECT ql2_id, site, property_id, hotel_name, creation_time, \
                               latitude, longitude, geobox, last_match_date, automatch_comment\
                               from QL2_HOTELMATCH.PUBLIC.DAILY_GEOBOX_MATCH \
                               where ql2_day = ('{ql2_day}') and status <> '171' and site <> '3333' \
                               order by creation_time desc ;"
    rows = run_snowflake_query( query )

    df = pd.DataFrame(rows, columns = ['ql2_id','site', 'property_id', 'hotel_name', 'creation_time', 'lat', 'lon',
                                       'geobox','last_match_date', 'automatch_comment'])
    df[["lat", "lon"]] = df[["lat", "lon"]].apply(pd.to_numeric)
    df[["creation_time", "last_match_date", "automatch_comment"]] = df[["creation_time", "last_match_date", "automatch_comment"]].astype(str)

    df['automatch_flg']=False
    df.loc[(df['automatch_comment'] != 'None') &
           (df['automatch_comment'] != '') &
           (df['automatch_comment'] is not None),
           'automatch_flg'] = True

    df['source']='Others'
    df.loc[df['site'] == 1, 'source'] = 'Expedia'
    df.loc[df['site'] == 33, 'source'] = 'Booking'
    df.loc[df['site'] == 620, 'source'] = 'Agoda'
    df.loc[df['site'] == 714, 'source'] = 'Trip Advisor'
    df.loc[df['site'] == 888, 'source'] = 'Trivago'
    df.loc[df['site'] == 10, 'source'] = 'Priceline'

    dfg = df.copy().iloc[:, [7]]
    dfg.sort_values(by=['geobox'], inplace=True)
    dfg.drop_duplicates(subset='geobox', keep='first', inplace=True)
    geolist = dfg['geobox'].to_numpy()
    
    return df, geolist

def set_distance(df, center_lat, center_lon, maxdistance):
    df['dlat'] = np.radians(center_lat) - np.radians(df['lat'])
    df['dlon'] = np.radians(center_lon) - np.radians(df['lon'])
    df['distance-3'] = np.power(np.sin(df['dlat'] / 2) , 2)
    df['distance-3'] += np.cos(np.radians(df['lat'])) * np.cos(df['dlat']) *  ( np.power( np.sin(df['dlon'] / 2) , 2 ))
    df['distance-2'] = 2 * np.arctan2(np.sqrt(df['distance-3']), np.sqrt(1 - df['distance-3']))
    df['distance-1'] = df['distance-2'] * 6373000
    df['distance'] = df['distance-1'].astype(int)
    df = df.loc[df['distance']<=maxdistance]

    return df

def get_onepage_hotel():
    return st.session_state['dfdaily'].loc[ st.session_state['dfdaily']['geobox'] == st.session_state.geobox_arr[st.session_state.page] ]

def set_arc_data(df):
    df_from = df.drop_duplicates(subset='ql2_id', keep='first', inplace=False)
    df_from['lat'] = df_from['lat']-0.00001
    dft = pd.merge(df_from, df, on='ql2_id', how='inner')
    return dft

def get_others(dfo):
    for key in st.session_state.site_dict:
        dfo=dfo[dfo['site'].astype(str)!=str(key)]
    return dfo

def set_a_dot_layer(title, data, color):
    ALL_LAYERS[title]= pdk.Layer(
        "ScatterplotLayer",
        data=data,
        get_position='[lon, lat]',
        auto_highlight=True,
        get_radius=10,
        get_fill_color=color,
        pickable=True,
    )

def set_arc_layers(title, df_arc):
    ALL_LAYERS[ title ] = pdk.Layer(
            "ArcLayer",
            data=df_arc,
            get_source_position=["lon_x", "lat_x"],
            get_target_position=["lon_y", "lat_y"],
            get_source_color=[20, 30, 250, 190],
            get_target_color=[30, 30, 230, 90],
            auto_highlight=True,
            width_scale=0.005,
            get_width="outbound",
            width_min_pixels=5,
            width_max_pixels=30,
    )

def set_names( title, df ):
    ALL_LAYERS[title] = pdk.Layer(
            "TextLayer",
            data=df,
            get_position=["lon", "lat"],
            get_text='hotel_name',
            get_color=[2, 2, 2, 250],
            get_size=15,
            get_alignment_baseline="'bottom'",
    )

def change_page( direction ):
    st.session_state.page = get_page_number(st.session_state['page'], st.session_state['total_nb_match'], direction)
    dfgeo = get_onepage_hotel()
    dfnew = dfgeo.loc[dfgeo['ql2_id'] == dfgeo.iloc[0]['ql2_id']]
    dfgeo = dfgeo.loc[dfgeo['ql2_id'] != dfgeo.iloc[0]['ql2_id']]
    center_lat = np.float(dfnew.iloc[[0]]['lat'] )
    center_lon = np.float(dfnew.iloc[[0]]['lon'] )
    dfgeo = set_distance(dfgeo, center_lat, center_lon, maxdistance)
    dfnew = set_distance(dfnew, center_lat, center_lon, 9999)
    st.session_state.dfgeo = dfgeo
    st.session_state.dfnew = dfnew

# Starts here
# dfdaily: all hotels in a geobox where a match occured that day
# dfgeo, dfnew: subset of dfdaily, hotels for one geobox
# data: subset of dfgeo or dfnew for one ql2_id

dist_arr = [-1, 50, 100, 200, 500]

# Init statefull session
if 'page' not in st.session_state:
    st.session_state.conn = init_snowflake_connection()
    st.session_state.site_dict = {1: "Expedia", 33: "Booking", 620: "Agoda", 714: "Trip Advisor", 888: "Trivago", 10:"Priceline",
                                  }
    st.session_state.colors = {"Expedia": [5, 5, 255, 220], "Booking": [225, 220, 4, 210], "Agoda": [255, 20, 4, 210], \
                               "Trip Advisor": [2, 170, 2, 210], "Trivago": [2, 220, 220, 110],
                               "Priceline": [245, 162, 54, 180], \
                               'Others': [233, 233, 0, 180]}

    st.session_state.mode = "Hotel" ; # can be group or detail
    st.session_state.maxdistance_index = 0
    st.session_state.maxdistance = dist_arr[st.session_state.maxdistance_index] ; # in meters

    datum = get_max_date()
    st.session_state['dfdaily'],st.session_state.geobox_arr = get_hotel_data(datum)
    st.session_state['page'] = 0
    st.session_state['total_nb_match'] = len(st.session_state.geobox_arr)
    st.session_state.dfgeo = get_onepage_hotel()
    st.session_state.dfnew = st.session_state.dfgeo.loc[st.session_state.dfgeo['ql2_id'] == st.session_state.dfgeo.iloc[0]['ql2_id']]
    center_lat = np.float(st.session_state.dfnew.iloc[[0]]['lat'])
    center_lon = np.float(st.session_state.dfnew.iloc[[0]]['lon'])
    st.session_state.dfgeo = set_distance(st.session_state.dfgeo, center_lat, center_lon, st.session_state.maxdistance)
    st.session_state.dfnew = set_distance(st.session_state.dfnew, center_lat, center_lon, 9999)
    st.session_state.extend = 'match'


maxdistance =st.session_state.maxdistance

# Button / Pagination changing geobox
Prev, Next = st.columns([3, 3])
if Next.button("Next Match"):
    change_page(1)
if Prev.button("Previous Match"):
    change_page(-1)

ranger, details = st.columns([3,3])
if ranger.button("More/Less surrounding hotels"):
    st.session_state.maxdistance_index += 1
    if st.session_state.maxdistance_index>=len(dist_arr):
        st.session_state.maxdistance_index = 0
    st.session_state.maxdistance = dist_arr[st.session_state.maxdistance_index]
    maxdistance = st.session_state.maxdistance
    change_page(0)
if details.button("Expand Matched/Surrounding Properties"):
    if st.session_state.extend == 'match':
        st.session_state.extend = 'surround'
    else:
        st.session_state.extend = 'match'

# Set local vars
maxdistance =st.session_state.maxdistance
geobox = st.session_state.geobox_arr[st.session_state.page]
dfgeo = st.session_state.dfgeo.reset_index()
dfnew = st.session_state.dfnew
center_lat = np.float(dfnew.iloc[[0]]['lat'])
center_lon = np.float(dfnew.iloc[[0]]['lon'])

###
# Map Layers
###
ALL_LAYERS = {}

###
## Build Surroundings
###
if st.session_state.extend == 'surround':
    # Extend mode
    for key in st.session_state.site_dict:
        source = st.session_state.site_dict[key]
        data = dfgeo.loc[dfgeo['site'].astype(str) == str(key)]
        #print( f"Build geo for {source} - len={len(data)}")
        if len(data)>0:
            set_a_dot_layer( f"{source} ({len(data)})", data,  st.session_state.colors[source])

    df_geo_others = get_others(dfgeo)
    if len(df_geo_others) > 0:
        set_a_dot_layer(f"Others ({len(df_geo_others)})", df_geo_others,  st.session_state.colors["Others"])

    set_names("Surrounding Hotel Names", dfgeo)
    df_garc = set_arc_data(dfgeo)
    set_arc_layers("Surround Hotel Arcs", df_garc)
    #print(f"Visible arcs={len(df_garc)}")

else:
    if len(dfgeo) > 0:
        dfgeo.drop_duplicates(subset='ql2_id', keep='first', inplace=True)
        dfgeo['source'] = 'Multiple'
        set_a_dot_layer(f"Unmatched Hotels within {maxdistance}m ({len(dfgeo)})", dfgeo, [125, 125, 128, 200])


###
# Build Layers for matched
###
if st.session_state.extend == 'match':
    # Match extend mode
    for key in st.session_state.site_dict:
        source = st.session_state.site_dict[key]
        data = dfnew.loc[dfnew['site'].astype(str) == str(key)]
        if len(data)>0:
            set_a_dot_layer(f"Match {source} ({len(data)})", data,  st.session_state.colors[source])

    df_others = get_others(dfnew)
    if len(df_others) > 0:
        set_a_dot_layer(f"Match Others ({len(df_others)})", df_others,  st.session_state.colors["Others"])

else:
    #df1ew = dfnew.groupby(['ql2_id'])['source','lon'].agg(lambda x: ','.join(x.dropna())).reset_index()
    #print(df1ew)
    dfnew.drop_duplicates(subset='ql2_id', keep='first', inplace=True)
    dfnew['source']='Multiple'
    set_a_dot_layer(f"Matched Property", dfnew, [125, 125, 128, 200])

df_arc = set_arc_data(dfnew)
set_arc_layers("Matched Hotel Arcs", df_arc)
set_names("Matched Hotel Names", dfnew)


#print(f"Visible matched dots={len(dfnew)}")
#print(f"Visible large group hotel dots={len(dfgeo)}")

# Add Layers to sidebar
st.sidebar.markdown("## Sites")
selected_layers = []
for layer_name, layer in ALL_LAYERS.items():
    if st.sidebar.checkbox(layer_name, True):
        selected_layers.append(layer)

# Display Header
if maxdistance>0:
    msg=f"and Surrounding properties within {maxdistance} meters"
else:
    msg=f" not showing surrounding properties"
st.write(f"Match # {st.session_state.page+1} \
       / {st.session_state.total_nb_match} \
       - {dfnew.iloc[0]['source']}: {dfnew.iloc[0]['hotel_name']} \
       \n{msg}")

# Display map
if selected_layers:
    st.pydeck_chart(pdk.Deck(
         map_style='mapbox://styles/mapbox/light-v9',
         initial_view_state=pdk.ViewState(
         latitude=center_lat,
         longitude=center_lon,
         zoom=16,
         pitch=10,
     ),
     layers=selected_layers,
     tooltip={
            'html':'{source} : {hotel_name} \
                <br>Created on : {creation_time} \
                <br>Last matched on : {last_match_date} \
                    <br>Auto Matcher : {automatch_flg} \
                    <br>ids : {site}-{property_id} ql2_id : {ql2_id}, \
                    <br>distance : {distance} ',
                    'style': {
                        'color': 'white'
                    }
            },
     ))

st.write(f"----- Properties machting {dfnew.iloc[0]['property_id']} -----")
st.write(f" (lat:{center_lat} long:{center_lon} geobox:{geobox})"  )
st.write(dfnew)
st.write(f"----- Surrounding properties -----")
st.write(dfgeo)
