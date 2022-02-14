import base64
import datetime
from datetime import date
from threading import Thread

import launch_request
import streamlit as st
import pandas as pd
import numpy as np
import platform
import subprocess
import mongo_connect
import time
import subprocess
import markdown_rq

###############
# """Headers """
MONGO_EMOJI_URL = "https://servicenav.coservit.com/wp-content/uploads/2021/05/29.jpg"
st.set_page_config(page_title="Mongodb Requests initiator", page_icon=MONGO_EMOJI_URL, layout='wide')
st.title('GDELT Datas on MongoDB Cluster')
col1, mid, col2 = st.columns([10, 10, 10])
st.sidebar.image('static/mongodb.PNG', width=200)
# with mid: st.image('static/29.jpg', width=100)
###############

###############
# """ Offline or Online"""
if 'offline' not in st.session_state:
    st.session_state['offline'] = False
###############


###############
# """ Getting the Country and languages """
df_codes = pd.read_csv('static/language-codes-full_csv.csv')
codes = list(df_codes['English'])
df_country = pd.read_csv('static/Codification_pays.txt')
country_name = list(df_country['Name'])
CLUSTER_COMPS = ['tp-hadoop-9', 'tp-hadoop-10', 'tp-hadoop-16', 'tp-hadoop-24', 'tp-hadoop-25', 'tp-hadoop-35',
                 'tp-hadoop-36']
###############

print(st.session_state)


def checking_cluster_status():
    if not st.session_state['precheck']:
        st.session_state['perform_scan'] = True

    st.session_state['perform_scan'] = False


def btn_connect():
    button_pre_check = st.sidebar.button('🔍 | Perform cluster pre-check ')
    checking_cluster = st.sidebar.button('🔧 | Perform ops on cluster ')

    ip = st.sidebar.text_input("MongoDB IP", value="mongodb://localhost:27017/", max_chars=None, key=None,
                               type="default")
    if button_pre_check and not checking_cluster:
        pre_check()
    if checking_cluster and not button_pre_check:
        st.write('Cluster scan')
        # z = st.sidebar.button('start')
        for i in range(len(CLUSTER_COMPS)):
            st.write(
                f" {CLUSTER_COMPS[i]} - Response {ping(CLUSTER_COMPS[i])} - Ping : {continious_ping(CLUSTER_COMPS[i])}")
            time.sleep(1)

    button_connect = st.sidebar.button('Connection to MongoDB')
    if button_connect:
        connect_progress = st.sidebar.markdown("""Connection in progress""")
        connection_response = mongo_connect.connect_db()
        mongo_gif = st.sidebar.image('https://www.pistalix.in/wp-content/uploads/2018/11/mongodb.gif', width=100)
        if not connection_response[0]:
            st.sidebar.markdown("""Pymongo : unable to connect to the server""")
            connect_progress.empty()
            st.session_state['connection_ready'] = True
            return connection_response
        else:
            time.sleep(1)
            msg_connected = st.sidebar.markdown(f'✔️ Connected to MongoDB Server at : {ip}')
            connect_progress.empty()
            mongo_gif.empty()
            st.session_state['connection_ready'] = True
            return connection_response


def continious_ping(host):
    param = '-n' if platform.system().lower() == 'windows' else '-c'
    proc = (subprocess.Popen(['ping', param, '1', host], stdout=subprocess.PIPE))
    proc.wait()
    out, err = proc.communicate()
    # return out.decode('utf-8')
    return out.decode('utf-8')


def ping(host):
    param = '-n' if platform.system().lower() == 'windows' else '-c'
    return subprocess.call(['ping', param, '1', host]) == 0


def pre_check():
    print(st.session_state)

    if not st.session_state['perform_scan']:
        st.session_state['precheck'] = True
        my_bar = st.progress(0)
        st.write('Starting cluster pre-check')
        col_front1_pc, mid_front_pc, col_front2_pc = st.columns([50, 1, 35])
        progress = 0
        for computer in CLUSTER_COMPS:
            with col_front1_pc:
                try:
                    pinged = ping(computer)
                    if pinged:
                        st.success(f'{computer} connexion established ✅')
                        bad = False
                    else:
                        st.error(f'{computer} seems down  ❌')
                        bad = False
                except:
                    st.error(f'{computer} seems down  ❌')
            with col_front2_pc:
                if pinged:
                    st.success('ping : 1 ms')
                else:
                    st.error(f'ping down 🔗')
            progress += int(100 / len(CLUSTER_COMPS))
            my_bar.progress(progress)
            time.sleep(1)
        my_bar.progress(100)
        if not bad:
            col_front1_pc, mid_front_pc, col_front2_pc = st.columns([1, 50, 1])
            with mid_front_pc:
                st.markdown("## Cluster configuration : ")
                st.image('static/cluster2.PNG', width=1000)
    print(st.session_state)

    st.session_state['precheck'] = False


def button_pressed():
    if st.session_state.connection_ready or st.session_state['offline']:
        select_event = st.sidebar.selectbox('Which query do you want to run ?',
                                            ['', 'Query_1', 'Query_2', 'Query_3', 'Query_4'])
        if select_event == '':
            st.sidebar.markdown("""""")
        else:
            request_selected(select_event)


def request_selected(select_event):
    st.markdown(f'#### Initializing {select_event}')
    if select_event == 'Query_1':
        launch_request_1()
    elif select_event == 'Query_2':
        launch_request_2()
    elif select_event == 'Query_3':
        launch_request_3()
    elif select_event == 'Query_4':
        launch_request_4()


def launch_request_1():
    st.sidebar.markdown("""Nombre d’articles / évènements qu’il y a eu pour chaque triplet (jour, pays de l’évènement, 
            langue de l’article""")

    markdown_rq.mk_rq1()

    if st.session_state['connection_type'] == 'offline':
        data_request1 = pd.read_csv('csv/request1.csv')
        st.dataframe(data=data_request1, width=None, height=None)

    elif st.session_state['connection_type'] == 'online':
        d = st.sidebar.date_input("Event day", datetime.datetime(2021, 1, 8))
        st.sidebar.write('')
        #         d = datetime.date.today()
        dd = datetime.datetime(year=d.year, month=d.month, day=d.day)
        formatted_date = d.strftime("%A %d %B %Y")

        lang_chosen = st.sidebar.selectbox(f"Article language :", codes, index=123)
        code_chosen = df_codes['alpha3-b'][df_codes['English'] == lang_chosen].values[0]
        country_chosen = st.sidebar.selectbox('Country of the event :', country_name, index=83)
        full_country_chosen = df_country['FIPS 10-4'][df_country['Name'] == country_chosen].values[0]
        limit = st.sidebar.number_input('Limit Number', 2000, 100000, 5000)

        st.markdown(f"**Parameters** :   ")
        st.markdown(
            f"Event date : **{formatted_date}** - Article Language : **{lang_chosen}** - Country Chosen : **{country_chosen}**")
        st.markdown(f"---")

        online_data_request1 = launch_request.request_one(day=dd, country=full_country_chosen,
                                                          lang=code_chosen, limit=limit)
        if online_data_request1.empty:
            st.markdown("""### No result for this request""")
        else:
            st.dataframe(data=online_data_request1, width=None, height=None)


def launch_request_2():
    st.sidebar.markdown("""Pour un pays donné en paramètre, évènements qui y ont eu place triées par le
     nombre de mentions (tri décroissant). \n Agrégation par jour/mois/année""")

    markdown_rq.mk_rq2()

    if st.session_state['connection_type'] == 'offline':
        data_request2 = pd.read_csv('csv/request2.csv')
        st.dataframe(data=data_request2, width=None, height=None)

    elif st.session_state['connection_type'] == 'online':
        st.sidebar.write('')

        country_chosen = st.sidebar.selectbox('Country of the event : ', country_name, index=83)
        full_country_chosen = df_country['FIPS 10-4'][df_country['Name'] == country_chosen].values[0]
        granularity = st.sidebar.radio("Choose granularity", ('Day', 'Month', 'Year'))
        granularity_passed = 'm' if granularity == 'Month' else 'd' if granularity == 'Day' else 'y'
        threshold = st.sidebar.number_input('Threshold', 1, 100, 50)
        limit_glob = st.sidebar.number_input('Global limit events per granularity', 1, 1000, 15)

        st.markdown(f"**Parameters** :   ")
        st.markdown(f"Country chosen : **{country_chosen}** - Aggregation : **{granularity}**")
        st.markdown(f"---")

        online_data_request2 = launch_request.request_two(country=full_country_chosen,
                                                          granularity=granularity_passed,
                                                          mentions_treshold=threshold,
                                                          limit_events_per_granularity=limit_glob)
        if online_data_request2.empty:
            st.markdown("""### No result for this request""")
        else:
            st.dataframe(data=online_data_request2, width=None, height=None)


def launch_request_3():
    st.sidebar.markdown("""Pour une source de données (gkg.SourceCommonName) \n Thèmes, 
    personnes, lieux dont les articles de cette sources parlent ainsi que le nombre d’articles et le ton moyen des 
    articles (pour chaque thème/personne/lieu). \n Agrégation par jour/mois/année.""")

    markdown_rq.mk_rq3()

    if st.session_state['connection_type'] == 'offline':
        data_request3 = pd.read_csv('csv/request3.csv')
        st.dataframe(data=data_request3, width=None, height=None)

    elif st.session_state['connection_type'] == 'online':
        st.sidebar.write('')

        source = st.sidebar.text_input('Source : ', 'lorrain')
        granularity = st.sidebar.radio("Choose granularity", ('Day', 'Month', 'Year'), 1)
        granularity_passed = 'm' if granularity == 'Month' else 'd' if granularity == 'Day' else 'y'
        var = st.sidebar.radio("Variable choice", ('Thematics', 'Persons', 'Location'), 1)
        limit = st.sidebar.number_input('Limit Number', 1000, 15000, 5000)

        st.markdown(f"**Parameters** :   ")
        st.markdown(f"Source : **{source}** - Aggregation : **{granularity}** - Variable : **{var}**")
        st.markdown(f"---")

        if var == 'Thematics':
            var_passed = 'theme'
        elif var == 'Persons':
            var_passed = 'persons'
        else:
            var_passed = 'locs'

        online_data_request3 = launch_request.request_three(source=source,
                                                            granularity=granularity_passed, var=var_passed,
                                                            limit_total_responses=limit)
        if online_data_request3.empty:
            st.markdown("""### No result for this request""")
        else:
            st.dataframe(data=online_data_request3, width=None, height=None)


def launch_request_4():
    st.sidebar.markdown("""Evolution des relations entre deux pays (specifies en paramètre) au cours de 
    l’année. Vous pouvez vous baser sur la langue de l’article, le ton moyen des articles, les themes plus souvent 
    citées, les personalités ou tout element qui vous semble pertinent.  """)

    markdown_rq.mk_rq4()

    if st.session_state['connection_type'] == 'offline':
        data_request4 = pd.read_csv('csv/request4.csv')
        st.dataframe(data=data_request4, width=None, height=None)

    elif st.session_state['connection_type'] == 'online':
        d_debut = st.sidebar.date_input("Analysis starting date", datetime.datetime(2021, 1, 1))
        st.sidebar.write('')
        dd_debut = datetime.datetime(year=d_debut.year, month=d_debut.month, day=d_debut.day)
        formatted_date_debut = dd_debut.strftime("%A %d %B %Y")

        d_fin = st.sidebar.date_input("Analysis ending date", datetime.datetime(2021, 9, 1))
        st.sidebar.write('')
        dd_fin = datetime.datetime(year=d_fin.year, month=d_fin.month, day=d_fin.day)
        formatted_date_fin = dd_fin.strftime("%A %d %B %Y")

        country_chosen_1 = st.sidebar.selectbox('First country :', country_name, 201,
                                                key=1)
        full_country_chosen_1 = df_country['FIPS 10-4'][df_country['Name'] == country_chosen_1].values[0]
        country_chosen_2 = st.sidebar.selectbox('Second country :', country_name, 83,
                                                key=2)
        full_country_chosen_2 = df_country['FIPS 10-4'][df_country['Name'] == country_chosen_2].values[0]
        n_limit = st.sidebar.number_input('Limit Number', 1, 15, 5)

        st.markdown(f"**Parameters** :   ")
        st.markdown(
            f"Start date : {formatted_date_debut} - End date : {formatted_date_fin} - First country : **{country_chosen_1}** - Second country : **{country_chosen_2}** - Limit : **{n_limit}**")
        st.markdown(f"---")

        online_data_request4 = launch_request.request_four(n_limit=n_limit,
                                                           pays_1=full_country_chosen_1,
                                                           pays_2=full_country_chosen_2, date_min=d_debut,
                                                           date_max=d_fin)
        if online_data_request4 is None:
            st.markdown("""### No result for this request""")
        else:
            st.dataframe(data=online_data_request4, width=None, height=None)


def start():
    if 'connection_ready' not in st.session_state:
        st.session_state['connection_ready'] = False
    if 'coll' not in st.session_state:
        st.session_state['coll'] = ''
    if 'connection_type' not in st.session_state:
        st.session_state['connection_type'] = 'online'
    if 'perform_scan' not in st.session_state:
        st.session_state['perform_scan'] = False
    if 'precheck' not in st.session_state:
        st.session_state['precheck'] = False
    if 'startping' not in st.session_state:
        st.session_state['startping'] = False
    offline_box = st.sidebar.checkbox('Offline requests', value=False)
    st.session_state['offline'] = offline_box
    st.session_state['connection_type'] = 'offline' if st.session_state['offline'] else 'online'
    if not st.session_state['offline']:
        # checking_cluster_status()
        btn_connect()
        button_pressed()
    else:
        button_pressed()


start()
