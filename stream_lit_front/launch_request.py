import datetime
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st


def request_one(day, country, lang, limit):
    st.warning(f'➡️ | Starting the query n°1')

    start = datetime.datetime.now()

    cursor = st.session_state['coll'].aggregate([
        {"$match": {"$and": [{"date": day}, {"country": country}]}},
        {"$project": {"_id": 0, "ID": 1, "list_articles": 1, "num_mentions": 1}},
        {"$unwind": "$list_articles"},
        {"$match": {"list_articles.lang": lang}},
        {"$group": {"_id": [{"event": "$ID"}, {"num_mentions": "$num_mentions"}], "nb_articles": {"$count": {}}}},
        {"$sort": {"nb_articles": -1}},
        {"$limit": limit}
    ])
    duration1 = datetime.datetime.now() - start
    st.info(f"⏳ | Query processed in {duration1}")

    start = datetime.datetime.now()
    df_res = pd.DataFrame(list(cursor))
    new_col = ["event", "num_mentions"]
    if not df_res.empty:
        for i, col in enumerate(new_col):
            df_res.insert(i + 1, col, df_res.loc[:, '_id'].apply(pd.Series)[i])
            df_res.loc[:, col] = df_res.loc[:, col].apply(lambda x: x.get(col))
        df_res = df_res.drop('_id', axis=1)

        df_res = df_res.set_index("event")
        duration2 = datetime.datetime.now() - start
        st.info(f"⏳ | Results gathered in dataframe in {duration2}")
        st.success(f"✔️ | Total time : {duration1 + duration2}")
    return df_res


def request_two(country, granularity, mentions_treshold, limit_events_per_granularity):
    st.warning(f'➡️ | Starting the query n°2')
    mentions_treshold = 50  # US : 100, autre : 20
    limit_events_per_granularity = 15

    # VERSION MONGO-PANDAS
    start = datetime.datetime.now()
    cursor = st.session_state['coll'].aggregate([
        {"$match": {"$and": [{"country": country}, {"num_mentions": {"$gt": mentions_treshold}}]}},  # AVEC TRESHOLD
        {"$project": {"_id": 0,
                      "ID": 1,
                      "time": "$date",
                      "num_mentions": 1}},
    ])

    duration1 = datetime.datetime.now() - start
    st.info(f"⏳ | Query processed in {duration1}")

    start = datetime.datetime.now()
    df_res = pd.DataFrame(list(cursor))
    st.info(f"ℹ️ | Number of events returned before post-processing : {len(df_res)}")
    strf = '%Y/%m/%d' if granularity == 'd' else '%Y/%m' if granularity == 'm' else '%Y'
    if not df_res.empty:
        df_res["time"] = pd.to_datetime(df_res["time"]).dt.strftime(strf)
        df_res = df_res.sort_values(['time', 'num_mentions'], ascending=[True, False])
        df_res = df_res.set_index('time')
        df_res = df_res.groupby(df_res.index).head(limit_events_per_granularity)
        duration2 = datetime.datetime.now() - start

        st.info(f"⏳ | Results gathered in dataframe in {duration2}")

        st.success(f"✔️ | Total time : {duration1 + duration2}")
    return df_res


def request_three(source, granularity, var, limit_total_responses):
    st.warning(f'➡️ | Starting the query n°3')

    dico_rq3 = {"$group": {"_id": [{"source": "$list_articles.source"},
                                   {"$cond": [var == "theme",
                                              {f"{var}": f"${var}_base"},
                                              {f"{var}": f"$list_articles.{var}"}]},
                                   {"time": {
                                       "$cond": [granularity == 'd', {"$concat": [{"$toString": {"$year": "$date"}},
                                                                                  '/',
                                                                                  {"$toString": {"$month": "$date"}},
                                                                                  '/',
                                                                                  {"$toString": {
                                                                                      "$dayOfMonth": "$date"}}]},
                                                 {"$cond": [granularity == 'm',
                                                            {"$concat": [{"$toString": {"$year": "$date"}},
                                                                         '/',
                                                                         {"$toString": {"$month": "$date"}}]},
                                                            {"$toString": {"$year": "$date"}}]}]}}],
                           "nb_articles": {"$count": {}},
                           "avg_tone": {"$avg": {"$toDouble": "$list_articles.tone"}}}}

    start = datetime.datetime.now()

    if var == 'theme':
        cursor = st.session_state['coll'].aggregate([
            {"$match": {"list_articles.source": {"$regex": source}}},
            {"$unwind": "$list_articles"},
            {"$match": {"list_articles.source": {"$regex": source}}},
            dico_rq3,
            {"$sort": {"nb_articles": -1, "time": 1}},
            {"$limit": limit_total_responses}

        ])

    else:
        cursor = st.session_state['coll'].aggregate([
            {"$match": {"list_articles.source": {"$regex": source}}},
            {"$unwind": "$list_articles"},
            {"$unwind": f"$list_articles.{var}"},
            {"$match": {"list_articles.source": {"$regex": source}}},
            dico_rq3,
            {"$sort": {"nb_articles": -1, "time": 1}},
            {"$limit": limit_total_responses}

        ])

    # print_find(cursor)

    duration1 = datetime.datetime.now() - start
    st.info(f"⏳ | Query processed in {duration1}")

    start = datetime.datetime.now()
    df_res = pd.DataFrame(list(cursor))

    new_col = ["source", var, "time"]
    # transforme une colonne pandas composée d'une liste de dictionnaires en plusieurs colonnes
    if not df_res.empty:
        for i, col in enumerate(new_col):
            df_res.insert(i + 1, col, df_res.loc[:, '_id'].apply(pd.Series)[i])
            df_res.loc[:, col] = df_res.loc[:, col].apply(lambda x: x.get(col))

        df_res = df_res.drop('_id', axis=1)
        # df_test.loc[:, 'time'] = pd.to_datetime(df_test.loc[:, 'time'], format='%Y/%m').dt.month
        duration2 = datetime.datetime.now() - start

        st.info(f"⏳ | Results gathered in dataframe in {duration2}")

        st.success(f"✔️ | Total time : {duration1 + duration2}")
    return df_res


def request_four(n_limit, pays_1, pays_2, date_min, date_max):
    st.warning(f'➡️ | Starting the query n°4')
    global_start = datetime.datetime.now()

    # generation manuelle de la date_range
    date_min = pd.to_datetime(str(date_min))
    date_max = pd.to_datetime(str(date_max))
    date_range = pd.date_range(date_min, date_max, freq="MS")

    list_monthly_dfs = process_all_queries(date_range, n_limit, pays_1, pays_2)
    if list_monthly_dfs:
        global_df = generate_global_df(list_monthly_dfs, date_range, n_limit)
        print(global_df)
        display_evolution(global_df)

        global_duration = datetime.datetime.now() - global_start
        st.success(f"✔️ | Total time : {global_duration}")
        return global_df
    else:
        return None


def bloc_match_1(month_start, next_month_start, pays_1, pays_2):
    # util function used in all queries
    dico_bloc_match_1 = {"$and": [
        {"$or": [
            {"$and": [{"act1_country": pays_1}, {"act2_country": pays_2}]},
            {"$and": [{"act1_country": pays_2}, {"act2_country": pays_1}]}]},
        {"$and": [{"date": {"$gte": month_start}}, {"date": {"$lt": next_month_start}}]}
    ]}
    return dico_bloc_match_1


def process_all_queries(date_range, n_limit, pays_1, pays_2):
    # processes all queries (tone,articles,events,events_code, localisations, persons, sources) and return a list of dfs (one per month in daterange)

    queries_start = datetime.datetime.now()

    list_monthly_dfs = [pd.DataFrame() for i in range(len(date_range) - 1)]
    try:
        for i in range(len(date_range) - 1):
            month_start = date_range[i]
            next_month_start = date_range[i + 1]

            start = datetime.datetime.now()
            # nb d'events
            nb_events = st.session_state['coll'].count_documents(bloc_match_1(month_start, next_month_start, pays_1, pays_2))
            # nb d'articles
            cursor = st.session_state['coll'].aggregate([
                {"$match": bloc_match_1(month_start, next_month_start, pays_1, pays_2)},
                {"$unwind": "$list_articles"},
                {"$count": "nb_articles"}
            ])
            nb_articles = list(cursor)[0]['nb_articles']
            # ton moyen
            cursor = st.session_state['coll'].aggregate([
                {"$match": bloc_match_1(month_start, next_month_start, pays_1, pays_2)},
                {"$group": {"_id": "_id", "avg_evt_tone": {'$avg': "$tone"}}}
            ])
            avg_tone = list(cursor)[0]['avg_evt_tone']
            # regroupement
            _id = ["nb_events", "nb_articles", "avg_tone"]
            val = [nb_events, nb_articles, avg_tone]
            df_scalar = pd.DataFrame(zip(_id, val))
            df_scalar = df_scalar.set_axis(["_id", "val"], axis=1)
            duration1 = datetime.datetime.now() - start
            st.info(f"⏳ | {month_start.strftime('%Y-%m')} : scalar queries processed in {duration1}")

            # most common event_codes - exprimé en nombre d'events par type
            start = datetime.datetime.now()
            cursor = st.session_state['coll'].aggregate([
                {"$match": bloc_match_1(month_start, next_month_start, pays_1, pays_2)},
                {"$group": {"_id": "$theme_base", "val": {'$count': {}}}},
                {"$sort": {"val": -1}},
                {"$limit": n_limit}
            ])
            df_res_evt_code = pd.DataFrame(list(cursor))
            duration1 = datetime.datetime.now() - start
            st.info(f"⏳ | {month_start.strftime('%Y-%m')} : event type query processed in {duration1}")

            # top 3 people - exprimé en nombre d'articles citant chaque personne
            start = datetime.datetime.now()
            cursor = st.session_state['coll'].aggregate([
                {"$match": bloc_match_1(month_start, next_month_start, pays_1, pays_2)},
                {"$unwind": "$list_articles"},
                {"$unwind": "$list_articles.persons"},
                {"$group": {"_id": "$list_articles.persons", "val": {"$count": {}}}},
                {"$sort": {"val": -1}},
                {"$limit": n_limit}
            ])
            df_res_pers = pd.DataFrame(list(cursor))
            duration1 = datetime.datetime.now() - start
            st.info(f"⏳ | {month_start.strftime('%Y-%m')} : persons query processed in {duration1}")

            # top 3 organizations
            start = datetime.datetime.now()
            cursor = st.session_state['coll'].aggregate([
                {"$match": bloc_match_1(month_start, next_month_start, pays_1, pays_2)},
                {"$unwind": "$list_articles"},
                {"$unwind": "$list_articles.org"},
                {"$group": {"_id": "$list_articles.org", "val": {"$count": {}}}},
                {"$sort": {"val": -1}},
                {"$limit": n_limit}
            ])
            df_res_orgs = pd.DataFrame(list(cursor))
            duration1 = datetime.datetime.now() - start
            st.info(f"⏳ | {month_start.strftime('%Y-%m')} : organizations query processed in {duration1}")

            # top 3 locations
            start = datetime.datetime.now()
            cursor = st.session_state['coll'].aggregate([
                {"$match": bloc_match_1(month_start, next_month_start, pays_1, pays_2)},
                {"$unwind": "$list_articles"},
                {"$unwind": "$list_articles.locs"},
                {"$group": {"_id": "$list_articles.locs", "val": {"$count": {}}}},
                {"$sort": {"val": -1}},
                {"$limit": n_limit}
            ])
            df_res_locs = pd.DataFrame(list(cursor))
            duration1 = datetime.datetime.now() - start
            st.info(f"⏳ | {month_start.strftime('%Y-%m')} : locations query processed in {duration1}")

            # top 3 sources
            start = datetime.datetime.now()
            cursor = st.session_state['coll'].aggregate([
                {"$match": bloc_match_1(month_start, next_month_start, pays_1, pays_2)},
                {"$unwind": "$list_articles"},
                {"$group": {"_id": "$list_articles.source", "val": {"$count": {}}}},
                {"$sort": {"val": -1}},
                {"$limit": n_limit}
            ])
            df_res_src = pd.DataFrame(list(cursor))
            duration1 = datetime.datetime.now() - start
            st.info(f"⏳ | {month_start.strftime('%Y-%m')} : sources query processed in {duration1}")

            # vertical concatenation of all sub_df of the current month
            list_sub_df_per_month = [df_scalar, df_res_evt_code, df_res_pers, df_res_orgs, df_res_locs, df_res_src]
            for df in list_sub_df_per_month:
                list_monthly_dfs[i] = pd.concat([list_monthly_dfs[i], df], axis=0)

            # --Indexation of monthly df
            # index length < (3 + 4 * n_limit) if not enough rows matching some requests
            index = ["nb_events", "nb_articles", "avg_tone"]
            index.extend([f"event_type_{i}" for i in range(len(df_res_evt_code))])
            index.extend([f"person_{i}" for i in range(len(df_res_pers))])
            index.extend([f"org_{i}" for i in range(len(df_res_orgs))])
            index.extend([f"locs_{i}" for i in range(len(df_res_locs))])
            index.extend([f"source_{i}" for i in range(len(df_res_src))])
            list_monthly_dfs[i] = list_monthly_dfs[i].set_axis(index, axis=0)
            list_monthly_dfs[i]["val"] = list_monthly_dfs[i]["val"].round(1)

        queries_duration = datetime.datetime.now() - queries_start
        st.info(f"✔️ | Total query time : {queries_duration}")
        return list_monthly_dfs
    except:
        return None



def generate_global_df(list_monthly_dfs, date_range, n_limit):
    # gathers a list of monthly dataframes into a global readable dataframe

    global_df = pd.DataFrame()

    # reindexation de tous les dataframes à la taille théorique (utile dans le cas où il y aurait trop peu de valeurs retournées sur certaines dates)
    index = ["nb_events", "nb_articles", "avg_tone"]
    index.extend([f"event_type_{i}" for i in range(n_limit)])
    index.extend([f"person_{i}" for i in range(n_limit)])
    index.extend([f"org_{i}" for i in range(n_limit)])
    index.extend([f"locs_{i}" for i in range(n_limit)])
    index.extend([f"source_{i}" for i in range(n_limit)])

    # concatenation horizontale des dataframes asociés à chaque mois
    for df in list_monthly_dfs:
        df = df.reindex(index)
        global_df = pd.concat([global_df, df], axis=1)

    # renommage des colonnes en fonction du mois auquel elles correspondent
    columns = []
    for date in date_range[:-1]:
        columns.extend([f"{date.strftime('%Y-%m')}_id", f"{date.strftime('%Y-%m')}_val"])
    global_df = global_df.set_axis(columns, axis=1)

    return global_df


def display_evolution(global_df):
    # display evolution of nb articles, events and tone over the month of the analysis

    nb_events_evol = global_df.loc["nb_events", :].iloc[1::2].astype(float)
    nb_articles_evol = global_df.loc["nb_articles", :].iloc[1::2].astype(float)
    avg_tone_evol = global_df.loc["avg_tone", :].iloc[1::2].astype(float)

    fig = plt.figure(figsize=[10, 8])
    axes = fig.subplots(2, 1)
    st.line_chart(data=nb_events_evol, width=0, height=0, use_container_width=True)
    st.line_chart(data=nb_articles_evol, width=0, height=0, use_container_width=True)
    st.line_chart(data=avg_tone_evol, width=0, height=0, use_container_width=True)

    axes[0].plot(nb_events_evol, 'o-', label="nb events")
    axes[0].plot(nb_articles_evol, 'o-', label="nb articles")
    axes[0].set_ylim([0, max(nb_articles_evol) + 10])
    axes[0].set_title("Evolution du nombre d'events et d'articles au cours du temps")
    axes[0].legend()
    axes[1].plot(avg_tone_evol, 'o-', label="avg_tone", c="k")
    axes[1].set_ylim([-10, 10])
    axes[1].set_title("Evolution de l'average tone au cours du temps")
    axes[1].legend()


