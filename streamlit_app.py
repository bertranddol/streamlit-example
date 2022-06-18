from collections import namedtuple
import altair as alt
import math
import pandas as pd
import streamlit as st
import snowflake.connector

"""
# Welcome to Streamlit!

Edit `/streamlit_app.py` to customize this app to your heart's desire :heart:

If you have any questions, checkout our [documentation](https://docs.streamlit.io) and [community
forums](https://discuss.streamlit.io).

In the meantime, below is an example of what you can do with just a few lines of code:
"""
@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])

conn = init_connection()

@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

with st.echo(code_location='below'):
    rows = run_query("SELECT site, count(*) as cnt from ql2_prod.public.raw_hotels where ql2_qts = 7472 group by site order by cnt desc ;")

    df = pd.DataFrame (rows, columns = ['site','count'])
    df = df.head(10)
    df.set_index('site')
    df = pd.DataFrame(
     np.random.randn(50, 3),
     columns=["a", "b", "c"])
    
    st.bar_chart(df)

    # Print results.
    for row in rows:
        if row[1]>100:
            st.write(f"site {row[0]} = {row[1]} ")


    total_points = st.slider("Number of points in spiral", 1, 5000, 2000)
    num_turns = st.slider("Number of turns in spiral", 1, 100, 9)

    Point = namedtuple('Point', 'x y')
    data = []

    points_per_turn = total_points / num_turns

    for curr_point_num in range(total_points):
        curr_turn, i = divmod(curr_point_num, points_per_turn)
        angle = (curr_turn + 1) * 2 * math.pi * i / points_per_turn
        radius = curr_point_num / total_points
        x = radius * math.cos(angle)
        y = radius * math.sin(angle)
        data.append(Point(x, y))

    st.altair_chart(alt.Chart(pd.DataFrame(data), height=500, width=500)
        .mark_circle(color='#0068c9', opacity=0.5)
        .encode(x='x:Q', y='y:Q'))
