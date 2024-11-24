from tensorflow import keras
import os
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.layers import TFSMLayer

# Define early stopping
early_stop = keras.callbacks.EarlyStopping(monitor='val_loss', patience=20)


# Function to get the next available version number based on existing model versions
def get_next_version_number(model_dir):
    # List all files in the model directory
    existing_files = os.listdir(model_dir)
    version_numbers = []
    
    # Loop through the files to extract version numbers from model filenames like 'CLV_V1.keras'
    for file in existing_files:
        if file.startswith("CLV_V") and file.endswith(".keras"):
            try:
                # Extract version number from file name (e.g., 'CLV_V1.keras' -> 1)
                version_number = int(file.split("_V")[1].split(".")[0])
                version_numbers.append(version_number)
            except ValueError:
                pass
    
    # If no models exist, start with version 1, else increment the maximum version number by 1
    next_version = max(version_numbers, default=0) + 1
    return next_version


def load_and_finetune_model(model_path, X_train, y_train, epochs=1):
    """
    Load a pre-trained model, freeze layers, and fine-tune it on new data.

    Args:
        model_path (str): Path to the pre-trained model.
        X_train (numpy.ndarray): Training features.
        y_train (numpy.ndarray): Training labels.
        epochs (int): Number of epochs for fine-tuning.

    Returns:
        keras.Model: The fine-tuned model.
    """
    model = None
    try:
        # Load the pre-trained model
        model = keras.models.load_model(model_path, compile=False)
        print("Model loaded successfully.")
    except Exception as e:
        print(f"Error loading model: {e}")
        return None
    
    # Check model summary
    model.summary()
    print(X_train.head())  # Displays the first 5 rows
    print(X_train.count())

    # Compile the model (if it wasn't compiled when loaded)
    model.compile(optimizer=Adam(learning_rate=0.001), 
                  loss='mean_squared_error', 
                  metrics=['mae'])  # Use the appropriate loss and metrics for your task


    # Freeze all layers except the output layer if you want fine-tuning
    for layer in model.layers[:-1]:  # Freeze all layers except the output layer
        layer.trainable = False

    # Fine-tuning: Train the model with new data
    history = model.fit(X_train, y_train,
                        epochs=epochs,
                        validation_split=0.2,
                        verbose=1,
                        callbacks=[early_stop])


   # Versioning: Get the next version number
    model_dir = '/home/nhtrung/CLV-Big-Data-Project/stream_process/model'
    version_number = get_next_version_number(model_dir)

    # Define the model filename using the version number
    model_filename = os.path.join(model_dir, f"CLV_V{version_number}.keras")

    # Save the fine-tuned model with a new version
    model.save(model_filename)
    print(f"Model saved to {model_filename}")
    
    return model

