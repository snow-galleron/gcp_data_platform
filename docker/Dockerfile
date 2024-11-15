# Utiliser une image de base Python officielle
FROM python:3.12-slim

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers de votre application dans le conteneur
COPY . /app

# Installer les dépendances de l'application
RUN pip install --no-cache-dir -r requirements.txt

# Nettoyer les fichiers temporaires
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Exposer le port sur lequel l'application écoute
EXPOSE 8080

# Définir la commande pour démarrer l'application
CMD ["python", "main.py"]
