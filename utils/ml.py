
from sklearn.decomposition import PCA
from sklearn.feature_selection import VarianceThreshold

from scipy.sparse import csr_matrix


def get_feature_importances(features, feature_names=None):

    if isinstance(features, csr_matrix):
        features = features.A

    feature_select = VarianceThreshold(threshold=0.0)
    feature_select.fit(features)

    return {
        (feature_names[i] if feature_names is not None else i): variance
         for i, variance in enumerate(feature_select.variances_)}

def indices_to_matrix(indices, shape, dtype=bool, sparse=True):
    data = [True] * len(indices)
    data = csr_matrix((data, tuple(zip(*indices))),
        shape=shape, dtype=dtype)
    if not sparse:
        data = data.A 
    return data
