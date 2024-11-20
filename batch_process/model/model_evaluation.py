from keras.callbacks import ReduceLROnPlateau

# Load the pre-trained model
model = load_model('best_model.h5')  # Load model saved previously

# Freeze the earlier layers (not training them)
for layer in model.layers[:-2]:  # Freezing all layers except the last 2
    layer.trainable = False

# Unfreeze the last layers to train
for layer in model.layers[-2:]:
    layer.trainable = True

# Set learning rate optimizer with a lower initial rate for fine-tuning
optimizer = tf.keras.optimizers.Adam(learning_rate=0.001)
model.compile(optimizer=optimizer, loss='mse', metrics=['mae', 'mse'])

# Learning rate scheduler
lr_scheduler = ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=3, min_lr=1e-6)

# Fine-tuning the model on new data
history = model.fit(X_new_batch, y_new_batch, epochs=5, batch_size=32, validation_data=(X_val, y_val),
                    callbacks=[lr_scheduler], verbose=1)

# Save the model after training
model.save('best_model.h5')

# Predictions
dnn_preds = model.predict(X_test).ravel()
