import numpy as np

FLOAT_PREC = 6
def boundaries_to_geo(boundaries):
    features = []
    for boundary in boundaries:
        features.append({'type': 'Feature',
           'properties': {},
           'geometry': {'type': 'Polygon',
            'coordinates':[[(round(x, FLOAT_PREC), round(y, FLOAT_PREC))
                    for x,y in b]
                        for b in boundary]}})
        #print('@', boundary[0])

    return {'type': 'FeatureCollection',
          'features':features}
def geo_to_boundaries(geo_dict):
    return [feat['geometry']['coordinates']
            for feat in geo_dict['features']]



def clean_up_line(points, deviation=0.1):
    # get a line of points. remove all points that lie on the same line (except the very outer ones)
    new_points = [points[0]]
    
    for idx, (point) in enumerate(points[1:-1], start=1):
        if np.any(np.abs(0.5* (points[idx-1] + points[idx+1]) - point) > deviation):
            new_points.append(point)
    new_points.append(points[-1])
    return new_points
