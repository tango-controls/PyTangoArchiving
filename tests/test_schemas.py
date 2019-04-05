import PyTangoArchiving as pta

pta.Schemas.load();

for k in pta.Schemas.keys():
    print(k)
