#ifndef _FFT_H
#define _FFT_H

#include <cmath>
typedef struct Complex {

    public:
    double real;
    double imag;

    Complex() {
        real = 0.0;
        imag = 0.0;
    }

    Complex(double r, double i) {
        real = r;
        imag = i;
    }

    Complex& operator +=(Complex c) {
        this->real += c.real;
        this->imag += c.imag;
        return *this;
    }

    Complex& operator = (Complex c) {
        this->real = c.real;
        this->imag = c.imag;
        return *this;
    }

    Complex operator * (Complex c) {
        return Complex((this->real * c.real) - (this->imag * c.imag), (this->real * c.imag) + (this->imag * c.real));
    }

    Complex& operator *= (Complex c) {
        this->real = (this->real * c.real) - (this->imag * c.imag);
        this->imag = (this->real * c.imag) + (this->imag * c.real);
        return *this;
    }

    Complex operator ~() {
	return Complex(this->real, -this->imag);
    }

    Complex operator / (double d) {
	return Complex(this->real/d, -this->imag/d);
    }

} Complex;


void ccorr(double* , double* , uint16_t , std::vector<double>&);
void ccorr(double* , double* , uint16_t , std::vector<float>&);
void cconv(double* , double* , uint16_t , std::vector<double>&);
void cconv(double* , double* , uint16_t , std::vector<float>&);
double sigmoid(double x);
double sigmoid_given_fun(double x);

#endif
