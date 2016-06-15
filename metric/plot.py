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
    trace3 = go.Bar(x=df.category, y=df.sum_dt_median/3600, name='median hours spent')
    trace4 = go.Bar(x=df.category, y=df.ndays_act_median, name='median days active')
    trace5 = go.Bar(x=df.category, y=df.sum_n_nonzeo_nforum_posts, name='# students posted')

    fig = tls.make_subplots(rows=1, cols=5, shared_xaxes=True, print_grid=False)
    fig.append_trace(trace1, 1, 1)
    fig.append_trace(trace2, 1, 2)
    fig.append_trace(trace3, 1, 3)
    fig.append_trace(trace4, 1, 4)
    fig.append_trace(trace4, 1, 5)

    fig['layout'].update(height=400, width=900, showlegend=False, title=title)
    fig['layout']['xaxis1'].update(showgrid=False)
    fig['layout']['yaxis1'].update(title='# of conversion')
    fig['layout']['yaxis2'].update(title='median grade', showgrid=False)
    fig['layout']['yaxis3'].update(title='median hours spent', showgrid=False)
    fig['layout']['yaxis4'].update(title='median days active', showgrid=False)
    fig['layout']['yaxis5'].update(title='# students posted', showgrid=False)

    py.iplot(fig)



def plot_pcstats(median, counts, pct_passing, viewed, title=None):
    """
    Plot students' behavior: median events (nplay_video, nproblem_check, nforum_posts)
    meidan ndays_act, median grade, pct_passing grouped by goals;
    counts: number of students for each goal
    """
    trace1 = go.Bar(x=median.index, y=median.nplay_video, name='video plays (%s)' % overall[0], showlegend=True)
    trace2 = go.Bar(x=median.index, y=median.nproblem_check, name='problem checks (%s)' % overall[1], showlegend=True)
    trace3 = go.Bar(x=median.index, y=median.nforum_posts, name='forum posts (%s)' % overall[2], showlegend=True)
    trace4 = go.Bar(x=median.index, y=median.ndays_act, name='days active', showlegend=False)
    trace5 = go.Bar(x=median.index, y=median.grade, name='grade', showlegend=False)
    trace6 = go.Bar(x=median.index, y=pct_passing, name='pct passing', showlegend=False)
    trace7 = go.Bar(x=median.index, y=counts, name='# of students', showlegend=False)

    fig = tls.make_subplots(rows=5, cols=1, print_grid=False,
                              subplot_titles=('median events', 'median days active (%s)' % overall[3], 
                                              'median grade (%s)' % overall[4], 'pct passing (%s)' % overall[5], 
                                              '# of students'))

    fig.append_trace(trace1, 1, 1)
    fig.append_trace(trace2, 1, 1)
    fig.append_trace(trace3, 1, 1)
    fig.append_trace(trace4, 2, 1)
    fig.append_trace(trace5, 3, 1)
    fig.append_trace(trace6, 4, 1)
    fig.append_trace(trace7, 5, 1)
    
    fig['layout']['xaxis1'].update(tickfont=dict(size=9))
    fig['layout']['xaxis2'].update(tickfont=dict(size=9))
    fig['layout']['xaxis3'].update(tickfont=dict(size=9))
    fig['layout']['xaxis4'].update(tickfont=dict(size=9))
    fig['layout']['xaxis5'].update(tickfont=dict(size=9))

    fig['layout'].update(barmode='stack', title=title, font=dict(size=12),
                         height=1050, width=700, margin=go.Margin(b=120))
    py.iplot(fig)  



def heatmap(df, title=None, width=700):
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
        yaxis=dict(autorange='reversed'), 
        width=width, height=500,
        margin=go.Margin(l=100, b=120),
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
    colors = ['rgb(77, 175, 74)', 'rgb(255, 127, 0)', 'rgb(55, 126, 184)', 'rgb(228, 26, 28)']
    for i in range(0, df.shape[1]):
        data.append(go.Bar(x=df.index, y=df.ix[:, i], marker=dict(color=colors[i]), name=names[i]))
        
    layout = go.Layout(
    width=width,
    margin=go.Margin(b=150),
    barmode=barmode,
    title=title)
    
    fig = go.Figure(data=data, layout=layout)
    py.iplot(fig)