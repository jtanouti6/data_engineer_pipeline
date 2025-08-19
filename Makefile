# ========== Variables ==========

PROJECT_DIR := $(CURDIR)
VENV_DIR := $(PROJECT_DIR)/venv
PYTHON := $(VENV_DIR)/bin/python
PIP := $(VENV_DIR)/bin/pip
REQUIREMENTS := requirements.txt

# ========== Commandes Make ==========

# 🔧 Crée l'environnement virtuel
venv:
	@echo "📦 Création de l'environnement virtuel..."
	python3 -m venv $(VENV_DIR)
	$(PIP) install --upgrade pip

# 📥 Installe les dépendances
install: venv
	@echo "📥 Installation des dépendances..."
	$(PIP) install -r $(REQUIREMENTS)

# 🚀 Exécution du pipeline principal
run:
	@echo "🚀 Lancement du pipeline complet..."
	bash orchestration/pipeline_master.sh

# 🧪 Lancement des validations qualité uniquement
validate:
	@echo "🧪 Contrôle qualité uniquement..."
	bash orchestration/quality_monitor.sh

# 🧼 Formattage du code avec black
format:
	@echo "🧼 Formatage avec black..."
	$(PIP) install black
	$(VENV_DIR)/bin/black processing/ transformations/ orchestration/

# ❌ Supprime le venv (précaution)
clean:
	@echo "🧹 Suppression de l'environnement virtuel..."
	rm -rf $(VENV_DIR)

# ♻️ Réinstalle proprement
reset: clean install

