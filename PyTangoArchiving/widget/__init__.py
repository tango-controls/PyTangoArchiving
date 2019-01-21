#from contexttoolbar import ContextToolBar
#from snapdialogs import SnapLoader, SnapSaver

#__all__ = ['snaps','trend','history']


try:
    import snaps
    from snaps import SnapLoader, SnapSaver
except Exception,e:
    print 'Unable to import PyTangoArchiving.widget.snaps: %s'%e
    
try:
    import trend
except Exception,e:
    print 'Unable to import PyTangoArchiving.widget.trend: %s'%e
    
try:
    import history
except Exception,e:
    print 'Unable to import PyTangoArchiving.widget.history: %s'%e


#THIS INIT FILE TRIES TO BE AS LIGHT AS POSSIBLE 

