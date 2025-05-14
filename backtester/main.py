from data_loader import load_data
from feature_engineer import add_features
from label_strategy import add_labels
from train_utils import train_model

def main():
    df = load_data()
    df = add_features(df)
    df = add_labels(df)
    train_model(df)

if __name__ == "__main__":
    main()