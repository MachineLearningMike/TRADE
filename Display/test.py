import matplotlib.pyplot as plt
import numpy as np
from scipy.ndimage.filters import gaussian_filter1d

def gaussian( x , s):
    return 1./np.sqrt( 2. * np.pi * s**2 ) * np.exp( -x**2 / ( 2. * s**2 ) )

myData = np.zeros(25)
myData[ 12 ] = 1
myGaussian = np.fromiter( (gaussian( x , 1 ) for x in range( -3, 4, 1 ) ), float )
filterdData = gaussian_filter1d( myData, 1 )

myFilteredData = np.convolve( myData, myGaussian, mode='same' )
fig = plt.figure(1)

ax = fig.add_subplot( 2, 1, 1 )
ax.plot( myData, marker='x', label='peak' )
ax.plot( filterdData, marker='^',label='filter1D smeared peak' )
ax.plot( myGaussian, marker='v',label='test Gaussian' )
ax.plot( myFilteredData, marker='v', linestyle=':' ,label='convolve smeared peak' )
ax.legend( bbox_to_anchor=( 1.05, 1 ), loc=2 )

B = [0.011,0.022,.032,0.027,0.025,0.033,0.045,0.063,0.09,0.13,0.17,0.21]
myGaussian = np.fromiter( ( gaussian( x , 2.09 ) for x in range( -4, 5, 1 ) ), float )
bx = fig.add_subplot( 2, 1, 2 )
bx.plot( B, label='data: B' )
bx.plot( gaussian_filter1d( B, 2.09 ), label='filter1d, refl' )
bx.plot( myGaussian, label='test Gaussian' )
bx.plot(  np.convolve( B, myGaussian, mode='same' ), label='Gaussian smear' )
bx.plot( gaussian_filter1d( B, 2.09, mode='constant' ), linestyle=':', label='filter1d, constant')
bx.legend( bbox_to_anchor=(1.05, 1), loc=2 )
plt.tight_layout()
plt.show()