from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

def train_model(df):
    X = df.select(["ma10", "std10"]).to_numpy()
    y = df["label"].to_numpy()

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
    model = RandomForestClassifier()
    model.fit(X_train, y_train)
    print(classification_report(y_test, model.predict(X_test)))
