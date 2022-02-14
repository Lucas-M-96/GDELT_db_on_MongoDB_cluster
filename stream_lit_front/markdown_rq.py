import streamlit as st


def mk_rq1():
    expand_rq1 = st.expander("Show the request", expanded=False)
    with expand_rq1:
        st.markdown("""```py
            cursor = coll.aggregate([
                {"$match": {"$and": [{"date": day}, {"country": country}]}},
                {"$project": {"_id": 0, "ID": 1, "list_articles": 1, "num_mentions": 1}},
                {"$unwind": "$list_articles"},
                {"$match": {"list_articles.lang": lang}},
                {"$group": {"_id": [{"event": "$ID"}, {"num_mentions": "$num_mentions"}], "nb_articles": {"$count": {}}}},
                {"$sort": {"nb_articles": -1}},
                {"$limit": limit_total_events}
                ]) """)


def mk_rq2():
    expand_rq2 = st.expander("Show the request", expanded=False)
    with expand_rq2:
        st.markdown("""```py
                    cursor = st.session_state['coll'].aggregate([
                    {"$match": {"$and": [{"country": country}, {"num_mentions": {"$gt": mentions_treshold}}]}}, 
                    {"$project": {"_id": 0,
                                  "ID": 1,
                                  "time": "$date",
                                  "num_mentions": 1}},
                    ]) """)


def mk_rq3():
    expand_rq3 = st.expander("Show the request", expanded=False)
    with expand_rq3:
        st.markdown("""```py
                dico_rq3 = {"$group": {"_id": [{"source": "$list_articles.source"},
                                      {"$cond": [var == "theme", {f"{var}": f"${var}_base"}, {f"{var}": f"$list_articles.{var}"}]},
                                      {"time": {"$cond": [granularity == 'd', {"$concat": [{"$toString": {"$year": "$date"}}, '/', {"$toString": {"$month": "$date"}}, '/', {"$toString": {"$dayOfMonth": "$date"}}]},
                                               {"$cond": [granularity == 'm', {"$concat": [{"$toString": {"$year": "$date"}}, '/', {"$toString": {"$month": "$date"}}]}, {"$toString": {"$year": "$date"}}]}]}}],
                                      "nb_articles": {"$count": {}},
                                      "avg_tone": {"$avg": {"$toDouble": "$list_articles.tone"}}}}


                if var == 'theme':
                    cursor = st.session_state['coll'].aggregate([
                        {"$match": {"list_articles.source": {"$regex": source}}},
                        {"$unwind": "$list_articles"},
                        {"$match": {"list_articles.source": {"$regex": source}}},
                        dico_rq3,
                        {"$sort": {"nb_articles": -1, "time": 1}}
                        {"$limit":n_limit}
                    ])
            
                else:
                    cursor = st.session_state['coll'].aggregate([
                        {"$match": {"list_articles.source": {"$regex": source}}},
                        {"$unwind": "$list_articles"},
                        {"$unwind": f"$list_articles.{var}"},
                        {"$match": {"list_articles.source": {"$regex": source}}},
                        dico_rq3,
                        {"$sort": {"nb_articles": -1, "time": 1}}
                        {"$limit":n_limit}
                    ]) """)


def mk_rq4():
    expand_rq3 = st.expander("Show the request", expanded=False)
    with expand_rq3:
        st.markdown("""```py

def bloc_match_1(month_start, next_month_start, pays_1, pays_2):
    dico_bloc_match_1 = {"$and": [{"$or": [{"$and": [{"act1_country": pays_1}, {"act2_country": pays_2}]},
                        {"$and": [{"act1_country": pays_2}, {"act2_country": pays_1}]}]},
                        {"$and": [{"date": {"$gte": month_start}}, {"date": {"$lt": next_month_start}}]}]}
    return dico_bloc_match_1


def process_all_queries(date_range, n_limit, pays_1, pays_2):
    list_monthly_dfs = [pd.DataFrame() for i in range(len(date_range) - 1)]
    for i in range(len(date_range) - 1):
        month_start = date_range[i]
        next_month_start = date_range[i + 1]

        # nb d'events
        nb_events = st.session_state['coll'].count_documents(bloc_match_1(month_start, next_month_start, pays_1, pays_2))
        # nb d'articles
        cursor = st.session_state['coll'].aggregate([{"$match": bloc_match_1(month_start, next_month_start, pays_1, pays_2)}, {"$unwind": "$list_articles"}, {"$count": "nb_articles"} ])
        nb_articles = list(cursor)[0]['nb_articles']
        # ton moyen
        cursor = st.session_state['coll'].aggregate([{"$match": bloc_match_1(month_start, next_month_start, pays_1, pays_2)}, {"$group": {"_id": "_id", "avg_evt_tone": {'$avg': "$tone"}}} ])
        avg_tone = list(cursor)[0]['avg_evt_tone']
        # regroupement
        _id = ["nb_events", "nb_articles", "avg_tone"]
        val = [nb_events, nb_articles, avg_tone]
        df_scalar = pd.DataFrame(zip(_id, val))
        df_scalar = df_scalar.set_axis(["_id", "val"], axis=1)

        # most common event_codes - exprimé en nombre d'events par type
        cursor = st.session_state['coll'].aggregate([
            {"$match": bloc_match_1(month_start, next_month_start, pays_1, pays_2)},
            {"$group": {"_id": "$theme_base", "val": {'$count': {}}}},
            {"$sort": {"val": -1}},
            {"$limit": n_limit}])
        df_res_evt_code = pd.DataFrame(list(cursor))

        cursor = st.session_state['coll'].aggregate([
            {"$match": bloc_match_1(month_start, next_month_start, pays_1, pays_2)},
            {"$unwind": "$list_articles"},
            {"$unwind": "$list_articles.persons"},
            {"$group": {"_id": "$list_articles.persons", "val": {"$count": {}}}},
            {"$sort": {"val": -1}},
            {"$limit": n_limit} ])
            
        df_res_pers = pd.DataFrame(list(cursor))

        # top 3 organizations
        start = datetime.datetime.now()
        cursor = st.session_state['coll'].aggregate([
            {"$match": bloc_match_1(month_start, next_month_start, pays_1, pays_2)},
            {"$unwind": "$list_articles"},
            {"$unwind": "$list_articles.org"},
            {"$group": {"_id": "$list_articles.org", "val": {"$count": {}}}},
            {"$sort": {"val": -1}},
            {"$limit": n_limit} ])
            
        df_res_orgs = pd.DataFrame(list(cursor))

        # top 3 locations
        cursor = st.session_state['coll'].aggregate([
            {"$match": bloc_match_1(month_start, next_month_start, pays_1, pays_2)},
            {"$unwind": "$list_articles"},
            {"$unwind": "$list_articles.locs"},
            {"$group": {"_id": "$list_articles.locs", "val": {"$count": {}}}},
            {"$sort": {"val": -1}},
            {"$limit": n_limit}  ])
        df_res_locs = pd.DataFrame(list(cursor))

        # top 3 sources
        cursor = st.session_state['coll'].aggregate([
            {"$match": bloc_match_1(month_start, next_month_start, pays_1, pays_2)},
            {"$unwind": "$list_articles"},
            {"$group": {"_id": "$list_articles.source", "val": {"$count": {}}}},
            {"$sort": {"val": -1}},
            {"$limit": n_limit} ])
        df_res_src = pd.DataFrame(list(cursor))

        list_sub_df_per_month = [df_scalar, df_res_evt_code, df_res_pers, df_res_orgs, df_res_locs, df_res_src]
        for df in list_sub_df_per_month:
            list_monthly_dfs[i] = pd.concat([list_monthly_dfs[i], df], axis=0)

        # --Indexation of monthly df
        index = ["nb_events", "nb_articles", "avg_tone"]
        index.extend([f"event_type_{i}" for i in range(len(df_res_evt_code))])
        index.extend([f"person_{i}" for i in range(len(df_res_pers))])
        index.extend([f"org_{i}" for i in range(len(df_res_orgs))])
        index.extend([f"locs_{i}" for i in range(len(df_res_locs))])
        index.extend([f"source_{i}" for i in range(len(df_res_src))])
        list_monthly_dfs[i] = list_monthly_dfs[i].set_axis(index, axis=0)
        list_monthly_dfs[i]["val"] = list_monthly_dfs[i]["val"].round(1)

    return list_monthly_dfs


def generate_global_df(list_monthly_dfs, date_range, n_limit):
    # gathers a list of monthly dataframes into a global readable dataframe

    global_df = pd.DataFrame()

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

    columns = []
    for date in date_range[:-1]:
        columns.extend([f"{date.strftime('%Y-%m')}_id", f"{date.strftime('%Y-%m')}_val"])
    global_df = global_df.set_axis(columns, axis=1)

    return global_df""")
