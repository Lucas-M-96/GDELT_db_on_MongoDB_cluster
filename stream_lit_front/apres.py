def extract_date_range():
    # extraction of relevant daterange (monthly analysis of the entire database)
    start = datetime.datetime.now()
    cursor = coll.aggregate([
        {"$group": {"_id": "_id", "date_max": {"$max": "$date"}}}
    ])
    date_max = list(cursor)[0]['date_max']
    cursor = coll.aggregate([
        {"$group": {"_id": "_id", "date_min": {"$min": "$date"}}}
    ])
    date_min = list(cursor)[0]['date_min']
    date_range = pd.date_range(date_min.replace(day=1), date_max.replace(day=1).replace(month=(date_max.month + 1)),
                               freq="MS")
    duration1 = datetime.datetime.now() - start
    print(
        f"Initialisation : date range extracted in {duration1} - processing analysis from {date_range[0].strftime('%Y-%m')} to {date_range[-2].strftime('%Y-%m')}")
    return date_range


def bloc_match_1(month_start, next_month_start):
    # util function used in all queries
    dico_bloc_match_1 = {"$and": [
        {"$or": [
            {"$and": [{"act1_country": pays_1}, {"act2_country": pays_2}]},
            {"$and": [{"act1_country": pays_2}, {"act2_country": pays_1}]}]},
        {"$and": [{"date": {"$gte": month_start}}, {"date": {"$lt": next_month_start}}]}
    ]}
    return dico_bloc_match_1


def process_all_queries(date_range):
    # processes all queries (tone,articles,events,events_code, localisations, persons, sources) and return a list of dfs (one per month in daterange)

    queries_start = datetime.datetime.now()

    list_monthly_dfs = [pd.DataFrame() for i in range(len(date_range) - 1)]
    for i in range(len(date_range) - 1):
        month_start = date_range[i]
        next_month_start = date_range[i + 1]

        start = datetime.datetime.now()
        # nb d'events
        nb_events = coll.count_documents(bloc_match_1(month_start, next_month_start))
        # nb d'articles
        cursor = coll.aggregate([
            {"$match": bloc_match_1(month_start, next_month_start)},
            {"$unwind": "$list_articles"},
            {"$count": "nb_articles"}
        ])
        nb_articles = list(cursor)[0]['nb_articles']
        # ton moyen
        cursor = coll.aggregate([
            {"$match": bloc_match_1(month_start, next_month_start)},
            {"$group": {"_id": "_id", "avg_evt_tone": {'$avg': "$tone"}}}
        ])
        avg_tone = list(cursor)[0]['avg_evt_tone']
        # regroupement
        _id = ["nb_events", "nb_articles", "avg_tone"]
        val = [nb_events, nb_articles, avg_tone]
        df_scalar = pd.DataFrame(zip(_id, val))
        df_scalar = df_scalar.set_axis(["_id", "val"], axis=1)
        duration1 = datetime.datetime.now() - start
        print(f"{month_start.strftime('%Y-%m')} : scalar queries processed in {duration1}")

        # most common event_codes - exprimé en nombre d'events par type
        start = datetime.datetime.now()
        cursor = coll.aggregate([
            {"$match": bloc_match_1(month_start, next_month_start)},
            {"$group": {"_id": "$theme_base", "val": {'$count': {}}}},
            {"$sort": {"val": -1}},
            {"$limit": n_limit}
        ])
        df_res_evt_code = pd.DataFrame(list(cursor))
        duration1 = datetime.datetime.now() - start
        print(f"{month_start.strftime('%Y-%m')} : event type query processed in {duration1}")

        # top 3 people - exprimé en nombre d'articles citant chaque personne
        start = datetime.datetime.now()
        cursor = coll.aggregate([
            {"$match": bloc_match_1(month_start, next_month_start)},
            {"$unwind": "$list_articles"},
            {"$unwind": "$list_articles.persons"},
            {"$group": {"_id": "$list_articles.persons", "val": {"$count": {}}}},
            {"$sort": {"val": -1}},
            {"$limit": n_limit}
        ])
        df_res_pers = pd.DataFrame(list(cursor))
        duration1 = datetime.datetime.now() - start
        print(f"{month_start.strftime('%Y-%m')} : persons query processed in {duration1}")

        # top 3 organizations
        start = datetime.datetime.now()
        cursor = coll.aggregate([
            {"$match": bloc_match_1(month_start, next_month_start)},
            {"$unwind": "$list_articles"},
            {"$unwind": "$list_articles.org"},
            {"$group": {"_id": "$list_articles.org", "val": {"$count": {}}}},
            {"$sort": {"val": -1}},
            {"$limit": n_limit}
        ])
        df_res_orgs = pd.DataFrame(list(cursor))
        duration1 = datetime.datetime.now() - start
        print(f"{month_start.strftime('%Y-%m')} : organizations query processed in {duration1}")

        # top 3 locations
        start = datetime.datetime.now()
        cursor = coll.aggregate([
            {"$match": bloc_match_1(month_start, next_month_start)},
            {"$unwind": "$list_articles"},
            {"$unwind": "$list_articles.locs"},
            {"$group": {"_id": "$list_articles.locs", "val": {"$count": {}}}},
            {"$sort": {"val": -1}},
            {"$limit": n_limit}
        ])
        df_res_locs = pd.DataFrame(list(cursor))
        duration1 = datetime.datetime.now() - start
        print(f"{month_start.strftime('%Y-%m')} : locations query processed in {duration1}")

        # top 3 sources
        start = datetime.datetime.now()
        cursor = coll.aggregate([
            {"$match": bloc_match_1(month_start, next_month_start)},
            {"$unwind": "$list_articles"},
            {"$group": {"_id": "$list_articles.source", "val": {"$count": {}}}},
            {"$sort": {"val": -1}},
            {"$limit": n_limit}
        ])
        df_res_src = pd.DataFrame(list(cursor))
        duration1 = datetime.datetime.now() - start
        print(f"{month_start.strftime('%Y-%m')} : sources query processed in {duration1}")

        # vertical concatenation of all sub_df of the current month
        list_sub_df_per_month = [df_scalar, df_res_evt_code, df_res_pers, df_res_orgs, df_res_locs, df_res_src]
        for df in list_sub_df_per_month:
            list_monthly_dfs[i] = pd.concat([list_monthly_dfs[i], df], axis=0)

        # --Indexation of monthly df
        # index length will be < than (3 + 5 * n_limit) if not enough rows matching some requests
        index = ["nb_events", "nb_articles", "avg_tone"]
        index.extend([f"event_type_{i}" for i in range(len(df_res_evt_code))])
        index.extend([f"person_{i}" for i in range(len(df_res_pers))])
        index.extend([f"org_{i}" for i in range(len(df_res_orgs))])
        index.extend([f"locs_{i}" for i in range(len(df_res_locs))])
        index.extend([f"source_{i}" for i in range(len(df_res_src))])
        list_monthly_dfs[i] = list_monthly_dfs[i].set_axis(index, axis=0)
        list_monthly_dfs[i]["val"] = list_monthly_dfs[i]["val"].round(1)

    queries_duration = datetime.datetime.now() - queries_start
    print(f"Total query time : {queries_duration}")
    return list_monthly_dfs


def generate_global_df(list_monthly_dfs):
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
    axes[0].plot(nb_events_evol, 'o-', label="nb events")
    axes[0].plot(nb_articles_evol, 'o-', label="nb articles")
    axes[0].set_ylim([0, max(nb_articles_evol) + 10])
    axes[0].set_title("Evolution du nombre d'events et d'articles au cours du temps")
    axes[0].legend()
    axes[1].plot(avg_tone_evol, 'o-', label="avg_tone", c="k")
    axes[1].set_ylim([-10, 10])
    axes[1].set_title("Evolution de l'average tone au cours du temps")
    axes[1].legend()