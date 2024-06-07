from sklearn.model_selection import train_test_split
import xgboost as xgb
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit


def validate_model(model, X, y):
    tscv = TimeSeriesSplit(n_splits=6)

    mse_list = []
    r2_list = []
    print('Walidacja modelu:\n')
    for train_index, test_index in tscv.split(X):
        X_train, X_test = X.iloc[train_index], X.iloc[test_index]
        y_train, y_test = y.iloc[train_index], y.iloc[test_index]
        
        model.fit(X_train, y_train)
        
        y_pred = model.predict(X_test)
        
        mse = mean_squared_error(y_test, y_pred)
        mse_list.append(mse)
        r2 = r2_score(y_test, y_pred)
        r2_list.append(r2)
        print("Błąd średniokwadratowy (MSE):", mse)
        print("Współczynnik determinacji (R2):", r2)
        
    print("\nŚredni błąd średniokwadratowy (MSE) walidacji:", sum(mse_list)/len(mse_list))
    print("Średni współczynnik determinacji (R2) walidacji:", sum(r2_list)/len(r2_list))


def train_model(df):
    X = df.drop(columns=['rate'])
    y = df['rate']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.20, shuffle = False)

    model = xgb.XGBRegressor()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    print('Wyniki modelu:')
    print("Błąd średniokwadratowy (MSE):", mse)
    print("Współczynnik determinacji (R2):", r2, '\n\n')
    
    validate_model(model, X, y)
    return model
