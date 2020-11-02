import numpy as np
import os
from PIL import Image

def load_array(filename):
    """
    Given a valid image, load the image and return the pixels as a numpy array
    :param filename: The filename as a string
    :returns: A numpy array which stores the pixel data from a snowmap

    Convention is as follows: pixels that read 0,0,0, 255 are read as snow-free and contain the value 0;
    pixels that read 0,0,0,0 assume no data and return -1, and pixels that read (255, 255, 255, 255)
    are read as snow and get the value 1
    """

    image = Image.open(filename)
    image.load()
    height, width = image.size

    snowmap = np.zeros((height, width), dtype=int)
    for row in range(height):
        for col in range(width):
            a = image.getpixel((row,col))
            if a == (0, 0, 0, 255): # This is no snow
                snowmap[row, col] = 0
            elif a ==  (0, 0, 0, 0): # this is no data
                snowmap[row, col] = -1
            elif a ==  (255, 255, 255, 255): # that's for snow
                snowmap[row, col] = 1
            else:
                raise ValueError("Unknown Pixel value {}".format(a))
    return snowmap


def get_coordinates(filename):
    """
    Get coordinates from tiff file using gdal
    Assume they are printed something like this:
     Upper Left  (   5.8000000,  47.8900000) (  5d48' 0.00"E, 47d53'24.00"N)
     Lower Left  (   5.8000000,  45.6000000) (  5d48' 0.00"E, 45d36' 0.00"N)
     Upper Right (  12.7318760,  47.8900000) ( 12d43'54.75"E, 47d53'24.00"N)
     Lower Right (  12.7318760,  45.6000000) ( 12d43'54.75"E, 45d36' 0.00"N)
    """
    metadata_text = os.popen('gdalinfo {}'.format(filename)).readlines()
    for iline, line in enumerate(metadata_text):
        if line.strip() == 'Corner Coordinates:':
            break
    coords = []
    strings = ['Upper Left', 'Lower Left', 'Upper Right', 'Lower Right']
    for count, iline in enumerate(range(iline+1, iline+5)):
        # Quick check whether this is the correct line:
        assert metadata_text[iline].startswith(strings[count], "Wrong start of line: "+metadata_text[iline]
        coords.append(list(map(float, "".join(
                metadata_text[iline].split()[3:5]).lstrip('(').rstrip(')').split(','))))
    return np.array(coords)
