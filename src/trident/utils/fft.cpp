#include <iostream>
#include <vector>
#include <cmath>
#include <cassert>
#include <trident/utils/fft.h>
#include <cstring>
#include <unordered_map>
#include <mutex>
#include <kognac/logs.h>

using namespace std;

static std::mutex mylock;
static std::unordered_map<int, std::vector<double>> sinmap;
static std::unordered_map<int, std::vector<double>> cosmap;

static std::vector<double>& getCos(int n) {
    mylock.lock();
    std::vector<double>& cosn = cosmap[n];
    if (cosn.size() == 0) {
	LOG(DEBUGL) << "Computing cos " << n;
	for (int k = 0; k < n ; ++k){
	    double angle = 2 * M_PI * k / n;
	    cosn.push_back(std::cos(angle));
	}
    }
    mylock.unlock();
    return cosn;
}

static std::vector<double>& getSin(int n) {
    mylock.lock();
    std::vector<double>& sinn = sinmap[n];
    if (sinn.size() == 0) {
	LOG(DEBUGL) << "Computing sin " << n;
	for (int k = 0; k < n ; ++k){
	    double angle = 2 * M_PI * k / n;
	    sinn.push_back(std::sin(angle));
	}
    }
    mylock.unlock();
    return sinn;
}

void fft(std::vector<Complex> &in, std::vector<Complex> & out) {

    int n = in.size();
    std::vector<double>& cos = getCos(n);
    std::vector<double>& sin = getSin(n);

    for (int k = 0; k < n ; ++k){

        double realSum = 0.0;
        double imagSum = 0.0;
        for (int t = 0; t < n; ++t) {
            // double angle = 2 * M_PI * t * k / n;
	    // We use cos(-x) = cos(x) and sin(-x) = -sin(x)
            // double real = in[t].real * std::cos(-angle) - in[t].imag * std::sin(-angle);
            // double imag = in[t].imag * std::cos(-angle) + in[t].real * std::sin(-angle);
	    // So:
	    // double real = in.real * std::cos(angle) + in.imag * sin(angle)
	    // double imag = in.imag * std::cos(angle) - in.real * sin(angle)
            int angle = (t * k) % n;
            double real = in[t].real * cos[angle] + in[t].imag * sin[angle];
            double imag = in[t].imag * cos[angle] - in[t].real * sin[angle];
            realSum += real;
            imagSum += imag;
        }
        out[k].real = realSum;
        out[k].imag = imagSum;
    }
}

void fft(double *in, int n, std::vector<Complex> & out) {

    std::vector<double>& cos = getCos(n);
    std::vector<double>& sin = getSin(n);

    for (int k = 0; k < n ; ++k){

        double realSum = 0.0;
        double imagSum = 0.0;
        for (int t = 0; t < n; ++t) {
            // double angle = 2 * M_PI * t * k / n;
	    // We use cos(-x) = cos(x) and sin(-x) = -sin(x)
            // double real = in[t].real * std::cos(-angle) - in[t].imag * std::sin(-angle);
            // double imag = in[t].imag * std::cos(-angle) + in[t].real * std::sin(-angle);
	    // So:
	    // double real = in.real * std::cos(angle) + in.imag * sin(angle)
	    // double imag = in.imag * std::cos(angle) - in.real * sin(angle)
            int angle = (t * k) % n;
            double real = in[t] * cos[angle];
            double imag = - in[t]* sin[angle];
            realSum += real;
            imagSum += imag;
        }
        out[k].real = realSum;
        out[k].imag = imagSum;
    }
}

void ifft(std::vector<Complex> &in, std::vector<Complex> & out) {

    /*
     * 1. Take Complex conjugate of the input array , call it x'
     * 2. Take fft(x')
     * 3. Take Complext conjugate of fft(x')
     * 4. Divide it by N
     */
    for (int i = 0; i < in.size(); ++i) {
        in[i] = ~in[i];
    }

    fft(in, out);

    int n = out.size();
    for (int i = 0; i < out.size(); ++i) {
        out[i] = ~out[i];
        out[i] = out[i] / n;
    }
}

void ccorr(std::vector<Complex>& a, std::vector<Complex>& b, std::vector<double>& out) {
    /** Python code
    def ccorr(a,b):
        ifft(np.conj(fft(a)) * fft(b)).real
    */
    assert(a.size() == b.size());
    int n = a.size();

    // 1. Calculate FFT of vector a
    vector<Complex> aOut(n);
    fft(a, aOut);

    // 2. Calculate FFT of vector b
    vector<Complex> bOut(n);
    fft(b, bOut);

    // 3. Do complex multiplication of the conjufate of FFT(a) and FFT(b)
    for (int i = 0; i < n; ++i) {
        aOut[i] = ~aOut[i] * bOut[i];
    }

    // 5. Take inverse FFT of the result
    ifft(aOut, bOut);

    // Copy the output in the output vector
    out.resize(n);
    for (int i = 0; i < n; ++i) {
        out[i] = bOut[i].real;
    }
}

void ccorr(double* a, double* b, uint16_t size, vector<double>& out) {

    // 1. Calculate FFT of vector a
    vector<Complex> aOut(size);
    fft(a, size, aOut);

    // 2. Calculate FFT of vector b
    vector<Complex> bOut(size);
    fft(b, size, bOut);

    // 3. Do complex multiplication of the conjufate of FFT(a) and FFT(b)
    for (int i = 0; i < size; ++i) {
        aOut[i] = ~aOut[i] * bOut[i];
    }

    // 5. Take inverse FFT of the result
    ifft(aOut, bOut);

    // Copy the output in the output vector
    out.resize(size);
    for (int i = 0; i < size; ++i) {
        out[i] = bOut[i].real;
    }
}

void cconv(std::vector<Complex>& a, std::vector<Complex>& b, std::vector<double>& out) {
    /** Python code
    def ccov(a,b):
        ifft(fft(a) * fft(b)).real
    */
    assert(a.size() == b.size());
    int n = a.size();

    // 1. Calculate FFT of vector a
    vector<Complex> aOut(n);
    fft(a, aOut);

    // 2. Calculate FFT of vector b
    vector<Complex> bOut(n);
    fft(b, bOut);

    // 3. Do complex multiplication of aOut and FFT(b)
    for (int i = 0; i < n; ++i) {
        aOut[i] *= bOut[i];
    }

    // 4. Take inverse FFT of the result
    vector<Complex> inverseFFT(n);
    ifft(aOut, inverseFFT);

    // Copy the output in the output vector
    out.resize(n);
    for (int i = 0; i < n; ++i) {
        out[i] = inverseFFT[i].real;
    }
}

void cconv(double* a, double* b, uint16_t n, std::vector<double>& out) {

    // 1. Calculate FFT of vector a
    vector<Complex> aOut(n);
    fft(a, n, aOut);

    // 2. Calculate FFT of vector b
    vector<Complex> bOut(n);
    fft(b, n, bOut);

    // 3. Do complex multiplication of aOut and FFT(b)
    for (int i = 0; i < n; ++i) {
        aOut[i] *= bOut[i];
    }

    // 4. Take inverse FFT of the result
    vector<Complex> inverseFFT(n);
    ifft(aOut, inverseFFT);

    // Copy the output in the output vector
    out.resize(n);
    for (int i = 0; i < n; ++i) {
        out[i] = inverseFFT[i].real;
    }
}

void ccorr(double* a, double* b, uint16_t size, vector<float>& out) {
    vector<double> out_d;

    ccorr(a, b, size, out_d);

    out.resize(size);
    for (int i = 0; i < size; ++i) {
        out[i] = (float) out_d[i];
    }
}

void cconv(double* a, double* b, uint16_t size, std::vector<float>& out) {

    vector<double> out_d;

    cconv(a, b, size, out_d);

    out.resize(size);
    for (int i = 0; i < size; ++i) {
        out[i] = (float) out_d[i];
    }
}
