namespace TSnap {

int GetCommon(TIntV& A, TIntV& B) {
  int i, j;
  int ret = 0;
  int alen, blen;
  int d;
  TInt ai;

  alen = A.Len();
  blen = B.Len();
  i = 0;
  j = 0;
  if (i >= alen  ||  j >= blen) {
    return ret;
  }

  while (1) {
    d = A[i] - B[j];
    if (d < 0) {
      i++;
      if (i >= alen) {
        break;
      }
    } else if (d > 0) {
      j++;
      if (j >= blen) {
        break;
      }
    } else {
      ret++;
      i++;
      if (i >= alen) {
        break;
      }
      j++;
      if (j >= blen) {
        break;
      }
    }
  }
  return ret;
}

long GetCommon(const std::vector<long>& A, const std::vector<long>& B) {
  long i, j;
  long ret = 0;
  long alen, blen;
  long d;
  long ai;

  alen = A.size();
  blen = B.size();
  i = 0;
  j = 0;
  if (i >= alen  ||  j >= blen) {
    return ret;
  }

  while (1) {
    d = A[i] - B[j];
    if (d < 0) {
      i++;
      if (i >= alen) {
        break;
      }
    } else if (d > 0) {
      j++;
      if (j >= blen) {
        break;
      }
    } else {
      ret++;
      i++;
      if (i >= alen) {
        break;
      }
      j++;
      if (j >= blen) {
        break;
      }
    }
  }
  return ret;
}

} // namespace TSnap
