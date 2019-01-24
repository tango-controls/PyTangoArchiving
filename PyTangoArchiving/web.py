#!/usr/bin/python

import sys,time,traceback
import fandango
from fandango.functional import time2str
import pickle
import PyTangoArchiving
import fandango.qt
from fandango.qt import Qt

import guiqwt
import guidata
from guiqwt.plot import CurveDialog
from guiqwt.builder import make
from guiqwt.styles import COLORS


#ats = sys.argv[1:-2] #('bo01/vc/ccg-01/pressure','bo02/vc/ccg-02/pressure')
#import time
#t = [time.time()-24*3600,time.time()]
#t = sys.argv[-2],sys.argv[-1]

AllColors = ("aliceblue,antiquewhite,aqua,aquamarine,azure,beige,bisque,black,blanchedalmond,blue,"+\
            "blueviolet,brown,burlywood,cadetblue,chartreuse,chocolate,coral,cornflowerblue,cornsilk,"+\
            "crimson,cyan,darkblue,darkcyan,darkgoldenrod,darkgray,darkgreen,darkgrey,darkkhaki,darkmagenta,"+\
            "darkolivegreen,darkorange,darkorchid,darkred,darksalmon,darkseagreen,darkslateblue,"+\
            "darkslategray,darkslategrey,darkturquoise,darkviolet,deeppink,deepskyblue,dimgray,dimgrey,"+\
            "dodgerblue,firebrick,floralwhite,forestgreen,fuchsia,gainsboro,ghostwhite,gold,goldenrod,gray,"+\
            "green,greenyellow,grey,honeydew,hotpink,indianred,indigo,ivory,khaki,lavender,lavenderblush,"+\
            "lawngreen,lemonchiffon,lightblue,lightcoral,lightcyan,lightgoldenrodyellow,lightgray,lightgreen,"+\
            "lightgrey,lightpink,lightsalmon,lightseagreen,lightskyblue,lightslategray,lightslategrey,"+\
            "lightsteelblue,lightyellow,lime,limegreen,linen,magenta,maroon,mediumaquamarine,mediumblue,"+\
            "mediumorchid,mediumpurple,mediumseagreen,mediumslateblue,mediumspringgreen,mediumturquoise,"+\
            "mediumvioletred,midnightblue,mintcream,mistyrose,moccasin,navajowhite,navy,oldlace,olive,"+\
            "olivedrab,orange,orangered,orchid,palegoldenrod,palegreen,paleturquoise,palevioletred,"+\
            "papayawhip,peachpuff,peru,pink,plum,powderblue,purple,red,rosybrown,royalblue,saddlebrown,"+\
            "salmon,sandybrown,seagreen,seashell,sienna,silver,skyblue,slateblue,slategray,slategrey,snow,"+\
            "springgreen,steelblue,tan,teal,thistle,tomato,transparent,turquoise,violet,wheat,white,"+\
            "whitesmoke,yellow,yellowgreen").split(',')

DEFCOLORS = map(Qt.QColor,[
    'red',
    'blue',
    'grey',
    'brown',
    'darkgreen',
    'violet',
    'lightblue',
    'lightgreen',
    'orange',
    'magenta',
    ])

###############################################################################

#from PyTangoArchiving.utils import decimate_array,
from fandango.arrays import decimate_array,filter_array

###############################################################################

EXTENSION = '.png'
MAX_DATA_SIZE = 5e3

import re
get_varname = lambda p:re.sub('([-_]|(VAL(UE)?))','',('.' in p and p.split('/')[-1] or p).rsplit('.',1)[0].rsplit('--',1)[0]).replace('..','')

def get_plotable_values(attr,begin=0,end=0,decimate=True):
    print('Loading data for %s ...'%str(attr))
    if '.pck' in attr:
        #Reading a pickle file
        vals = pickle.load(open(attr))
    elif '.csv' in attr:
        #Reading a CSV file
        vals = [tuple(map(float,filter(bool,t.split())[:2])) for t in open(attr).readlines()]
    elif attr.count('/')==3:
        #Getting attribute values from archiving
        vals = reader.get_attribute_values(attr,t[0],t[1])
    elif '/' not in attr and '.' in attr:
        #Using a building variable
        try:
            import pybuilding
            vals = pybuilding.query_variable(attr)
        except:
            raise Exception('Unknown data source!')
    else:
        raise Exception('Unknown data source!')
    
    if begin:
        try:
            i = (i for i,t in enumerate(vals) if t[0]>=begin).next()
            vals = vals[i:]
        except: pass
    if end:
        try:
            i = (i for i,t in enumerate(vals) if t[0]>=end).next()
            vals = vals[:i]
        except: pass
    
    if decimate:
        if len(vals)>MAX_DATA_SIZE:
            vals = decimate_array(vals,fixed_size=MAX_DATA_SIZE,fixed_rate=100)
        
    return vals
    
###############################################################################

JS_PATH = 'http://www.cells.es/static/Files/Computing/Controls/Reports/js'
JS_INCLUDES = """
        <script language="javascript" type="text/javascript" src="$JS/jquery.min.js"></script>
        <script language="javascript" type="text/javascript" src="$JS/jquery.jqplot.min.js"></script>
        <script type="text/javascript" src="$JS/plugins/jqplot.canvasAxisTickRenderer.min.js"></script>
        <script type="text/javascript" src="$JS/plugins/jqplot.dateAxisRenderer.min.js"></script>
        <link rel="stylesheet" type="text/css" href="$JS/jquery.jqplot.css" />
        """.replace('$JS',JS_PATH)

def jqplot(title,vals,y2vals=None,xvals=None):
    #USING jqPlot instead of Qt
    ats = sorted(vals.keys())
    print 'JQPlot(%s,%s)'%(len(ats),','.join(ats))
    js = JS_PATH
    includes = JS_INCLUDES
    jqplot = """
        <div id="chartdiv" style="height:100%;width:100%; "></div>
        <script class="code" type="text/javascript">
        //var line1=[['2008-08-12 4:00',4], ['2008-09-12 4:00',6.5], ['2008-10-12 4:00',5.7], ['2008-11-12 4:00',9], ['2008-12-12 4:00',8.2]];
        //var line1 = [['2012-09-17 16:44', -0.24086535644531001], ['2012-09-17 16:44', -0.166169769287108], ['2012-09-17 16:45', -0.097435409545898494]];
        //var line1 = [['2012-09-17 16:41:25', -0.0238617248535157], ['2012-09-17 16:45:34', 0.058192413330078102], ['2012-09-17 16:49:34', 0.19318386840820501], ['2012-09-17 16:49:45', 0.61706387329101398], ['2012-09-17 16:49:55', 1.0387241058349601], ['2012-09-17 16:50:15', 1.54242512512208], ['2012-09-17 16:50:35', 2.4866759948730399], ['2012-09-17 16:51:34', 4.2881499938964902], ['2012-09-17 17:39:05', 2.0870143585204999], ['2012-09-17 17:39:15', -0.115877944946289], ['2012-09-17 17:43:55', -0.216508895874022], ['2012-09-17 17:50:45', -0.12760966491699099], ['2012-09-17 17:51:05', 0.00132557678222655], ['2012-09-17 17:51:14', 0.093648117065429706], ['2012-09-17 17:51:25', 0.17557904052734499], ['2012-09-17 17:51:35', 0.27481381225586199], ['2012-09-17 17:51:45', 0.45713497924804802], ['2012-09-17 17:52:05', 0.70768925476073896], ['2012-09-17 17:52:24', 1.0928863220214899], ['2012-09-17 17:52:55', 1.6552524261474699], ['2012-09-17 17:54:24', 2.6534446411132699], ['2012-09-17 17:57:35', 4.5955463104248198], ['2012-09-17 18:00:45', 7.7440131835937498], ['2012-09-17 19:02:15', 12.3284885101318], ['2012-09-17 19:15:35', 7.1876571350097702], ['2012-09-17 19:15:45', 0.51328236389160098], ['2012-09-17 19:16:35', 0.82798764038085604], ['2012-09-17 19:18:05', 1.2241496734619199], ['2012-09-17 19:18:25', 1.97309834289551], ['2012-09-17 19:18:45', 3.0986022644042799], ['2012-09-17 19:19:25', 4.5590980224609501], ['2012-09-17 19:19:55', 6.3049014739990499], ['2012-09-17 19:21:15', 10.757810562133701], ['2012-09-17 19:22:15', -0.19701274108886499], ['2012-09-17 19:22:25', 0.40055233764648701], ['2012-09-17 19:22:35', 1.10074002075196], ['2012-09-17 19:22:45', 1.6407546691894599], ['2012-09-17 19:23:15', 3.0096213989257699], ['2012-09-17 19:23:45', 4.4580032043457098], ['2012-09-17 19:24:25', 6.4163531951904602], ['2012-09-17 19:25:55', 10.671424835205], ['2012-09-17 19:27:35', 16.0758376770019], ['2012-09-17 19:36:35', -0.088338500976562498], ['2012-09-17 19:37:25', -0.0030716247558593901], ['2012-09-17 19:38:05', 0.081846588134765599], ['2012-09-17 19:38:45', 0.18200032043457201], ['2012-09-17 19:39:25', 0.24985005187988499]];
        line1 = $DATA;
        //var ticks = [[1,'Dec 10'], [2,'Jan 11'], [3,'Feb 11'], [4,'Mar 11'], [5,'Apr 11'], [6,'May 11'], [7,'Jun 11'], [8,'Jul 11'], [9,'Aug 11'], [10,'Sep 11'], [11,'Oct 11'], [12,'Nov 11'], [13,'Dec 11']]; 
        $(document).ready(function(){
            var plot1 = $.jqplot('chartdiv',  line1,
            { title:'$TITLE',
                //axes:{yaxis:{min:-10, max:240}},
                axes:{
                    xaxis:{
                        //ticks: ticks,
                        renderer:$.jqplot.DateAxisRenderer,
                        //min: "09-01-2008 16:00",
                        //max: "06-22-2009 16:00",
                        //rendererOptions:{
                        //        tickInset: 0,
                        //        tickRenderer:$.jqplot.CanvasAxisTickRenderer
                        //    },                        
                        tickOptions:{
                            formatString:'%b %e',
                            angle: -40
                            },
                                // For date axes, we can specify ticks options as human
                                // readable dates.  You should be as specific as possible,
                                // however, and include a date and time since some
                                // browser treat dates without a time as UTC and some
                                // treat dates without time as local time.
                                // Generally, if  a time is specified without a time zone,
                                // the browser assumes the time zone of the client.
                        //tickInterval: "2 weeks",
                        //tickRenderer: $.jqplot.CanvasAxisTickRenderer,
    
                        label:'Time(s)',
                        labelRenderer: $.jqplot.CanvasAxisLabelRenderer
                        },
                    yaxis:{
                        label:'Am',
                        labelRenderer: $.jqplot.CanvasAxisLabelRenderer
                        }
                    },
                $SERIES,
                legend:{
                    show:true,
                    placement: 'outsideGrid',
                    //location: 'e',
                    }
            });
        });
        </script>
        """
    serie = """
            {
                label:'$ATTR',
                lineWidth: 3,
                //color:'#5FAB78',
                color: "$COLOR",
                showMarker:false,
                //fill:true,
                //fillAndStroke:true,
            }
        """#.replace('$ATTR',CURRENT).replace('$COLOR','rgba(255, 0, 0, 0.5)')
    series = 'series:[\n%s\n]'%',\n'.join([
        serie.replace('$ATTR',a).replace('$COLOR','rgba(%d,%d,%d,1)'%DEFCOLORS[i].getRgb()[:3])
        for i,a in enumerate(ats)
        ])
    #data = """[[[1, 2],[3,5.12],[5,13.1],[7,33.6],[9,85.9],[11,219.9]]]"""
    max_size = int(float(MAX_DATA_SIZE)/len(vals))
    for k,v in vals.items():
        if len(v)>max_size:
            raise Exception('a warning must be added to notify that value are filtered or decimated')
            'the filter_array window should depend of times requested and max_size'
            vals[k] = decimate_array(v,fixed_size=max_size,fixed_rate=100)
    data = str([
        list([fandango.time2str(t[0]),t[1]] for t in vals[k]) for k in ats]
        )
    return jqplot.replace('$DATA',data).replace('$SERIES',series).replace('$TITLE',title)

if __name__=='__main__':
    args = sys.argv[1:]
    filename = (a for a in args if 'html' in a).next()
    title = ([a.split('=',1)[-1] for a in args if a.startswith('--title=')] or ['Data Report'])[0]
    model = [a for a in args if not a.startswith('--') and not a.endswith('.html')]
    vals = dict((get_varname(m) or m,get_plotable_values(m,decimate=False)) for m in model)
    for k in vals.keys():
        if not vals[k]: vals.pop(k)
    print('Writing %s report ...'%filename)
    f = open(filename,'w')
    f.write('<html><head><title>%s</title>%s</head>'%(title,JS_INCLUDES))
    f.write('<body>%s</body></html>'%jqplot(title,vals))
    f.close()

#win.show()
#win.exec_()


"""
66: brush = curve.brush()
67: brush
68: brush.setStyle(Qt.Qt.Dense6Pattern)
69: plot.replot()
70: brush.color
71: brush.color()
72: Qt.Qt.green
73: color= Qt.QColor(Qt.Qt.green)
74: l = Qt.QLabel('hello')
75: l.show()
76: f = l.font()
77: f.setColor(c)
78: l.text()
79: p = l.palette()
80: p.setColor(Qt.Qt.ForegroundRole,c)
81: p.setColor(p.Text,c)
82: c
83: p.setColor(p.Text,color)
84: l.show()
85: l.setPalette(p)
86: l.hide()
87: l.show()
88: p.setColor(p.WindowText,color)
89: l.setPalette(p)
90: _ip.magic("hist 100")
"""




