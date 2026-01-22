
from __future__ import annotations

import pendulum
import logging

from airflow.models.dag import DAG
from airflow.decorators import task

# You may need to install these libraries in your Airflow environment
# pip install scikit-learn numpy
try:
    import numpy as np
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score
except ImportError:
    logging.warning("Could not import scikit-learn or numpy. The DAG will fail.")
    logging.warning("Please install them with 'pip install scikit-learn numpy'")


@task
def generate_data():
    """Generates sample data for a binary classification problem."""
    X = np.random.rand(100, 2)  # 100 samples, 2 features
    y = (X[:, 0] + X[:, 1] > 1).astype(int)  # Simple linear boundary
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42
    )
    # Airflow's TaskFlow API will handle passing these values
    return {
        "X_train": X_train.tolist(),
        "X_test": X_test.tolist(),
        "y_train": y_train.tolist(),
        "y_test": y_test.tolist(),
    }


@task
def train_model(data: dict):
    """Trains a simple logistic regression model."""
    X_train = np.array(data["X_train"])
    y_train = np.array(data["y_train"])
    
    model = LogisticRegression()
    model.fit(X_train, y_train)
    
    # The model object itself isn't directly serializable in the same way.
    # For this example, we'll pass its parameters (coefficients and intercept).
    # In a real-world scenario, you would save the model to a file storage/model registry.
    return {
        "coef": model.coef_.tolist(),
        "intercept": model.intercept_.tolist()
    }


@task
def evaluate_model(model_params: dict, data: dict):
    """Evaluates the model and logs its accuracy."""
    X_test = np.array(data["X_test"])
    y_test = np.array(data["y_test"])

    # Recreate the model from parameters
    model = LogisticRegression()
    model.coef_ = np.array(model_params["coef"])
    model.intercept_ = np.array(model_params["intercept"])
    model.classes_ = np.array([0, 1]) # Manually set classes

    predictions = model.predict(X_test)
    accuracy = accuracy_score(y_test, predictions)

    logging.info(f"Model Accuracy: {accuracy:.2f}")
    return accuracy


with DAG(
    dag_id="simple_data_science_dag",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "data-science"],
) as dag:
    # 1. Generate the data
    data_dict = generate_data()
    
    # 2. Train the model using the generated data
    trained_model_params = train_model(data_dict)
    
    # 3. Evaluate the model
    evaluate_model(trained_model_params, data_dict)
