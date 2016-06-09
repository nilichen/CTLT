import pandas as pd

import plotly
import plotly.offline as py
import plotly.graph_objs as go
import plotly.tools as 
py.init_notebook_mode()

def plot_pev(pev, course_id, title=None):
    """
    Plot students' behavior: median_grade, median ndays_act, # students posted
    grouped by passing vs. explored vs. viewed;
    Example course_id: 'UBCx/Climate101x/3T2015'
    """
    query = \
    """Select n_verified_id, certified_sum, explored_sum, viewed_sum, registered_sum  
    From [courses.broad_stats_by_course] 
    Where course_id = '%s'""" % course_id
    stats =  pd.io.gbq.read_gbq(query, project_id='ubcxdata', verbose=False)
    stats.columns = ['nverified', 'npassing', 'nexplored', 'nviewed', 'nregistered']
    df = pev[pev.course_id == course_id]


    trace1 = go.Bar(x=stats.values[0], y=stats.columns, orientation='h', name='# of conversion')
    trace2 = go.Bar(x=df.category, y=df.grade_median, name='median grade')
    trace3 = go.Bar(x=df.category, y=df.ndays_act_median, name='median ndays_act')
    trace4 = go.Bar(x=df.category, y=df.sum_n_nonzeo_nforum_posts, name='# students posted')

    fig = tls.make_subplots(rows=1, cols=4, shared_xaxes=True)
    fig.append_trace(trace1, 1, 1)
    fig.append_trace(trace2, 1, 2)
    fig.append_trace(trace3, 1, 3)
    fig.append_trace(trace4, 1, 4)

    fig['layout'].update(height=400, width=800, showlegend=False, title=title)
    fig['layout']['xaxis1'].update(showgrid=False)
    fig['layout']['yaxis1'].update(title='# of conversion')
    fig['layout']['yaxis2'].update(title='median grade', showgrid=False)
    fig['layout']['yaxis3'].update(title='median ndays_act', showgrid=False)
    fig['layout']['yaxis4'].update(title='# students posted', showgrid=False)

    py.iplot(fig)



def plot_pcstats(median, counts, pct_passing, title=None):
    """
    Plot students' behavior: median events (nplay_video, nproblem_check, nforum_posts)
    meidan ndays_act, median grade, pct_passing grouped by goals;
    counts: number of students for each goal
    """
    trace0 = go.Bar(x=median.index, y=counts, name='# of students')
    trace1 = go.Bar(x=median.index, y=median.nplay_video, name='nplay_video')
    trace2 = go.Bar(x=median.index, y=median.nproblem_check, name='nproblem_check')
    trace3 = go.Bar(x=median.index, y=median.nforum_posts, name='nforum_posts')
    trace4 = go.Bar(x=median.index, y=median.ndays_act, name='ndays_act')
    trace5 = go.Bar(x=median.index, y=median.grade, name='grade')
    trace6 = go.Bar(x=median.index, y=pct_passing, name='pct_passing')

    fig = tls.make_subplots(rows=5, cols=1,
                              subplot_titles=('# of students', 'median events (nplay_video, nproblem_check, nforum_posts)',
                                              'median ndays_act', 'median grade', 'pct_passing'))
    fig.append_trace(trace0, 1, 1)
    fig.append_trace(trace1, 2, 1)
    fig.append_trace(trace2, 2, 1)
    fig.append_trace(trace3, 2, 1)
    fig.append_trace(trace4, 3, 1)
    fig.append_trace(trace5, 4, 1)
    fig.append_trace(trace6, 5, 1)

    fig['layout']['xaxis1'].update(showticklabels=False)
    fig['layout']['xaxis2'].update(showticklabels=False)
    fig['layout']['xaxis3'].update(showticklabels=False)
    fig['layout']['xaxis4'].update(showticklabels=False)

    fig['layout'].update(barmode='stack', showlegend=False, title=title, 
                         height=800, width=600, margin=go.Margin(b=120))
    py.iplot(fig)    



def heatmap(df, title=None, width=800):
    """Plot heatmap given a dataframe"""
    data = [
        go.Heatmap(
            z=df.values,
            x=df.columns,
            y=df.index,
            colorscale=[[0.0, 'rgb(224,243,248)'], [1.0, 'rgb(43,130,189)']]
        )
    ]

    layout = go.Layout(
        width=width, height=500,
        margin=go.Margin(l=200, b=100),
        title=title
    )

    fig = go.Figure(data=data, layout=layout)
    py.iplot(fig)


def stacked_bar(df, names, barmode, title=None, width=600):
    """
    Plot stacked or overlay bar graph given the dataframe
    names: names for all the traces
    """
    data = []
    for i in range(1, df.shape[1]):
        data.append(go.Bar(x=df.ix[:, 0], y=df.ix[:, i], name=names[i-1]))
        
    layout = go.Layout(
    width=width,
    margin=go.Margin(b=100),
    barmode=barmode,
    title=title)
    
    fig = go.Figure(data=data, layout=layout)
    py.iplot(fig)