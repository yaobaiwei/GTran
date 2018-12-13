/*-----------------------------------------------------

       @copyright (c) 2018 CUHK Husky Data Lab
              Last modified : 2018-11
  Author(s) : Chenghuan Huang(entityless@gmail.com)
:)
-----------------------------------------------------*/

#include "mkl_util.hpp"
#include <fstream>
// #include "intrin.h"

using namespace std;

static __inline__ unsigned long long GetCycleCount()
{
    unsigned hi, lo;
    __asm__ volatile("rdtsc":"=a"(lo),"=d"(hi));
    return ((unsigned long long)lo)|(((unsigned long long)hi)<<32);
}

MKLUtil::MKLUtil(int tid)
{
    tid_ = tid;
    // printf("MKLUtil::MKLUtil() %d\n", tid_);

    //they are the same, 0.
    // if(tid == 0)
    // {
    //     printf("%d %d\n", VSL_ERROR_OK, VSL_STATUS_OK);
    // }

    int cc = GetCycleCount();
    // int cc = __rdtsc();
    vslNewStream(&rng_stream_, VSL_BRNG_SFMT19937, cc);
}

void MKLUtil::Test()
{
    double r[1000]; /* buffer for random numbers */
    double s; /* average */
    VSLStreamStatePtr stream;
    int i, j;

    /* Initializing */        
    s = 0.0;
    vslNewStream( &stream, VSL_BRNG_MT19937, 777 );

    /* Generating */        
    for ( i=0; i<10; i++ ) {

        vdRngGaussian( VSL_RNG_METHOD_GAUSSIAN_ICDF, stream, 1000, r, 5.0, 2.0 );
        for ( j=0; j<1000; j++ ) {
            s += r[j];
        }
    }
    s /= 10000.0;

    /* Deleting the stream */        
    vslDeleteStream( &stream );

    /* Printing results */        
    printf( "Sample mean of normal distribution = %f\n", s );
}

// vRngUniform
// Generates random numbers uniformly distributed over
// the interval [a, b).
// Syntax
// status = viRngUniform( method, stream, n, r, a, b );
// Include Files
// • mkl.h
// Input Parameters
// Name Type Description
// method const MKL_INT Generation method; the specific value is as follows:
// VSL_RNG_METHOD_UNIFORM_STD
// Standard method. Currently there is only one method for
// this distribution generator.
// stream VSLStreamStatePtr Pointer to the stream state structure
// n const MKL_INT Number of random values to be generated
// a const int Left interval bound a
// b const int Right interval bound b
// Output Parameters
// Name Type Description
// r int* Vector of n random numbers uniformly distributed over the
// interval [a,b)


void MKLUtil::UniformRNGI4(int* dst, int len, int min, int max)
{
    int status = viRngUniform(VSL_RNG_METHOD_UNIFORM_STD, rng_stream_, len, dst, min, max + 1);

    assert(status == VSL_STATUS_OK);
}


// Syntax
// status = vsRngUniform( method, stream, n, r, a, b );
// status = vdRngUniform( method, stream, n, r, a, b );
// Include Files
// • mkl.h
// Input Parameters
// Name Type Description
// method const MKL_INT Generation method; the specific values are as follows:
// VSL_RNG_METHOD_UNIFORM_STD
// VSL_RNG_METHOD_UNIFORM_STD_ACCURATE
// Standard method.
// stream VSLStreamStatePtr Pointer to the stream state structure
// n const MKL_INT Number of random values to be generated
// a const float for vsRngUniform
// const double for
// vdRngUniform
// Left bound a
// b const float for vsRngUniform Right bound b



void MKLUtil::UniformRNGF4(float* dst, int len, float min, float max)
{
    int status = vsRngUniform(VSL_RNG_METHOD_UNIFORM_STD, rng_stream_, len, dst, min, max);

    assert(status == VSL_STATUS_OK);
}


void MKLUtil::UniformRNGF8(double* dst, int len, double min, double max)
{
    int status = vdRngUniform(VSL_RNG_METHOD_UNIFORM_STD, rng_stream_, len, dst, min, max);

    assert(status == VSL_STATUS_OK);
}
