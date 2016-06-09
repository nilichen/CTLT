from __future__ import division
import pandas as pd


def compute_pcstats(merged, selections, start):
    """
    Compute students' behavior: median events (nplay_video, nproblem_check, nforum_posts)
    meidan ndays_act, median grade, pct_passing grouped by goals;
    merged: entry survey merged with person_course
    selections: a list of all the goals from entry survey
    start: index of the first column for this question in the entry survey
    """
    df = []
    rm = []
    counts = []
    pct_passing = []
    selections2 = selections[:]
    merged_count = len(merged)*0.05
    for i in range(len(selections2)):
        group_count = merged.ix[:, i+start].notnull().sum()

        if group_count > merged_count:
            counts.append(group_count)
            pct_passing.append(merged.ix[merged.ix[:, i+start].notnull(),'certified'].sum() / group_count)
            df.append(pd.DataFrame(data={'Selection': selections[i],
                                         'grade': merged.ix[merged.ix[:, i+start].notnull(), 'grade'],
                                         'nforum_posts': merged.ix[merged.ix[:, i+start].notnull(), 'nforum_posts'],
                                         'ndays_act': merged.ix[merged.ix[:, i+start].notnull(), 'ndays_act'], 
                                         'nproblem_check': merged.ix[merged.ix[:, i+start].notnull(), 'nproblem_check'],
                                         'nplay_video': merged.ix[merged.ix[:, i+start].notnull(), 'nplay_video']
                                         }))
        else:
            rm.append(selections2[i]) 
    result = pd.concat(df)
    #result.fillna(0, inplace=True)
    for s in rm:
        selections2.remove(s)
    median = result.groupby('Selection').median().ix[selections2, :]    
    return median, counts, pct_passing 



def compute_heatmap(surveys, selections, matrix, start, col_names):
    """
    Compute stats for questions 'Are you likely...' and 'How satisfied ...';
    surveys: entry survey merged with exit survey
    selections: a list of all the goals from entry survey
    start: index of the first column for question about goals in the merged survey
    matrix: for question Are you likely...' or 'How satisfied ...', 
    a dictionary with column names as keys and an empty list as values
    e.g. matrix = {'Q2_2_1': [], 'Q2_2_2': [], 'Q2_2_3': [], 'Q2_2_4': [], 'Q2_2_5': []}
    col_names: corresponding names for the columns
    e.g. ['Recommend', 'Advanced', 'Revisit', 'Involvement', 'Habbits']
    """
    rm = []
    selections2 = selections[:]
    for i in range(len(selections2)):
        group_count = surveys.ix[:, i+start].notnull().sum()
        survey_count = len(surveys)*0.05
        if group_count > survey_count:
            for k, v in matrix.iteritems():
                v.append(surveys.ix[surveys.ix[:, i+start].notnull(), k].notnull().sum() / group_count)
        else:
            rm.append(selections2[i])
    for s in rm:
        selections2.remove(s)
    matrix = pd.DataFrame(matrix)
    matrix.index = selections2
    matrix.columns = col_names
    return matrix


def cal_goalsmet(surveys, selections, start, col_name):
    """
    Calculate stats for goalsmet question;
    surveys: entry survey merged with exit survey
    selections: a list of all the goals from entry survey
    start: index of the first column for question about goals in the merged survey
    col_name: name of goalsmet column, e.g. 'Q2_1'
    """
    def cal_percent(df):
        return df.goalsmet.value_counts() / len(df)
    
    df = []
    for i in range(len(selections)):
        if surveys.ix[:, i+start].notnull().sum() > len(surveys)*0.05:
            df.append(pd.DataFrame(data={'Selection': selections[i],
                                         'goalsmet': surveys.ix[surveys.ix[:, i+start].notnull(), col_name]}))
            
    goalsmet = pd.concat(df)
    goalsmet = goalsmet.groupby('Selection').apply(cal_percent).unstack('Selection').fillna(0)
    goalsmet.loc["Didn't answer"] = 1 - goalsmet.sum(axis=0)
    col_order = ['Selection', 'Yes and more, the course exceeded my expectations.',
                'Yes, my goals were met.', 'My goals were somewhat met.', "Didn't answer"]
    return goalsmet.T.reset_index()[col_order]