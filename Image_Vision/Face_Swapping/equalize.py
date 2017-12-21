
from basic_packages import * 

def equalize(img):

    hist,bins = np.histogram(img.flatten(),256,[0,256])
    cdf = hist.cumsum()
    cdf_normalized = cdf * hist.max()/ cdf.max()
    cdf_m = np.ma.masked_equal(cdf,0)
    cdf_m = (cdf_m - cdf_m.min())*255/(cdf_m.max()-cdf_m.min())
    cdf = np.ma.filled(cdf_m,0).astype('uint8')
    return cdf[img]

    # clahe = cv2.createCLAHE(clipLimit=30.0, tileGridSize=(10,10))
    # cl1 = clahe.apply(img)
    # return cl1