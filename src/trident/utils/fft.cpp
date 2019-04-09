#include <iostream>
#include <vector>
#include <cassert>
#include <trident/utils/fft.h>
#include <cstring>
#include <unordered_map>
#include <mutex>
#include <kognac/logs.h>
#include <trident/utils/kiss_fft.h>
#include <trident/utils/kiss_fftr.h>

using namespace std;

static std::mutex mylock;
static std::unordered_map<int, kiss_fftr_cfg> real_regular_ffts;
static std::unordered_map<int, kiss_fft_cfg> complex_inverse_ffts;

static kiss_fftr_cfg get_real_regular(int n) {
    mylock.lock();

    kiss_fftr_cfg config = real_regular_ffts[n];
    if (config == NULL) {
        // LOG(INFOL) << "Creating real regular config for n = " << n;
        config = kiss_fftr_alloc(n, false, 0, 0);
        real_regular_ffts[n] = config;
    }
    mylock.unlock();
    return config;
}

static kiss_fft_cfg get_complex_inverse(int n) {
    mylock.lock();

    kiss_fft_cfg config = complex_inverse_ffts[n];
    if (config == NULL) {
        // LOG(INFOL) << "Creating complex inverse config for n = " << n;
        config = kiss_fft_alloc(n, true, 0, 0);
        complex_inverse_ffts[n] = config;
    }
    mylock.unlock();
    return config;
}

void fft_real_regular(kiss_fft_scalar *in, kiss_fft_cpx *out, int n) {

    kiss_fftr_cfg config = get_real_regular(n);
    kiss_fftr(config, in, out);
    for (int i = n/2 + 1; i < n; i++) {
        out[i].r = out[n - i].r;
        out[i].i = -out[n - i].i;
    }
    // LOG(INFOL) << "FFT: out[0].real = " << out[0].r << ", out[0].imag = " << out[0].i;
    // LOG(INFOL) << "FFT: out[n-1].real = " << out[n-1].r << ", out[n-1].imag = " << out[n-1].i;
}

void fft_complex_inverse(kiss_fft_cpx *in, kiss_fft_cpx *out, int n) {
    // LOG(INFOL) << "IFFT: in[0].real = " << in[0].r << ", in[0].imag = " << in[0].i;
    // LOG(INFOL) << "IFFT: in[n-1].real = " << in[n-1].r << ", in[n-1].imag = " << in[n-1].i;
    kiss_fft_cfg config = get_complex_inverse(n);
    kiss_fft(config, in, out);
    for (int i = 0; i < n; i++) {
        out[i].r /= n;
        out[i].i /= n;
    }
    // LOG(INFOL) << "IFFT: out[0].real = " << out[0].r << ", out[0].imag = " << out[0].i;
    // LOG(INFOL) << "IFFT: out[n-1].real = " << out[n-1].r << ", out[n-1].imag = " << out[n-1].i;
}

void ccorr(double* a, double* b, uint16_t size, vector<double>& out) {
    // Assume kiss_fft_scalar is defined as double.

    // 1. Calculate FFT of vector a
    vector<kiss_fft_cpx> aOut(size);
    fft_real_regular(a, &aOut[0], size);

    // 2. Calculate FFT of vector b
    vector<kiss_fft_cpx> bOut(size);
    fft_real_regular(b, &bOut[0], size);

    // 3. Do complex multiplication of the conjugate of FFT(a) and FFT(b)
    for (int i = 0; i < size; ++i) {
        kiss_fft_scalar re = aOut[i].r * bOut[i].r + aOut[i].i * bOut[i].i;
        kiss_fft_scalar im = -aOut[i].i * bOut[i].r + aOut[i].r * bOut[i].i;
        aOut[i].r = re;
        aOut[i].i = im;
    }

    // 5. Take inverse FFT of the result
    fft_complex_inverse(&aOut[0], &bOut[0], size);

    // Copy the output in the output vector
    out.resize(size);
    for (int i = 0; i < size; ++i) {
        out[i] = bOut[i].r;
    }
}

void cconv(double* a, double* b, uint16_t n, std::vector<double>& out) {

    // 1. Calculate FFT of vector a
    vector<kiss_fft_cpx> aOut(n);
    fft_real_regular(a, &aOut[0], n);

    // 2. Calculate FFT of vector b
    vector<kiss_fft_cpx> bOut(n);
    fft_real_regular(b, &bOut[0], n);

    // 3. Do complex multiplication of aOut and FFT(b)
    for (int i = 0; i < n; ++i) {
        kiss_fft_scalar re = aOut[i].r * bOut[i].r - aOut[i].i * bOut[i].i;
        kiss_fft_scalar im = aOut[i].i * bOut[i].r + aOut[i].r * bOut[i].i;
        aOut[i].r = re;
        aOut[i].i = im;
    }

    // 4. Take inverse FFT of the result
    fft_complex_inverse(&aOut[0], &bOut[0], n);

    // Copy the output in the output vector
    out.resize(n);
    for (int i = 0; i < n; ++i) {
        out[i] = bOut[i].r;
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
