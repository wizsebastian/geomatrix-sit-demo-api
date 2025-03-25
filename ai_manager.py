import os
import json
import openai
from openai import OpenAI  # Importar la clase principal del cliente


class AIAnalysisManager:
    def __init__(
        self,
    ):
        # self.api_key = os.environ.get("OPENAI_API_KEY")
        self.api_key = "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"

        if self.api_key is None:
            raise ValueError(
                "API key not found. Please provide it explicitly or set the OPENAI_API_KEY environment variable."
            )

        # Crear el cliente de OpenAI con la API key
        self.client = OpenAI(api_key=self.api_key)

        # Para depuración, mostrar los primeros 8 caracteres de la clave
        print(f"#########################API Key: {self.api_key[:8]}...")

    def load_prompt(self, prompt_name):
        # Resto del método igual
        prompt_path = os.path.join("prompts", f"{prompt_name}.json")
        if not os.path.exists(prompt_path):
            raise FileNotFoundError(f"Prompt file '{prompt_path}' not found.")

        with open(prompt_path, "r", encoding="utf-8") as f:
            prompt_data = json.load(f)

        analisis_data = prompt_data.get("prompt", {}).get("analisis")

        prompt_template = f"""
        Objetivo del análisis:
        {analisis_data["goal"]}

        Datos incluidos:
        {{datos_incluidos}}

        """

        return prompt_template

    def analyze(self, prompt_name, input_data, debug=False):
        try:
            # Convertir los datos a JSON legible
            datos_json = json.dumps(input_data, ensure_ascii=False, indent=2)

            # Generar un prompt simple directamente con los datos
            filled_prompt = f"""Por favor, analiza los siguientes datos agrícolas y proporciona un análisis detallado. 
Contexto: estos datos fueron recolectados de arcGis sobre predios, junto con servicios técnicos realizados en dichos predios.
Objetivo: Responde con una lista de recomendaciones. Identifica ineficiencias, riesgos , integrando datos de lotes y servicios técnicos.
Restricciones:Tu respuesta debe ser solo en base a los datos proporcionados. No divagues.
Datos:
{datos_json}"""
            if debug:
                print("=== Prompt generado ===")
                print(filled_prompt)
                print("=======================")

            # Llamada a OpenAI usando el cliente
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {
                        "role": "system",
                        "content": "Eres un asistente experto en análisis de datos agrícolas. Realiza un análisis detallado y proporciona recomendaciones si es posible.",
                    },
                    {"role": "user", "content": filled_prompt},
                ],
                temperature=0.7,
                max_tokens=1000,
            )

            return response.choices[0].message.content

        except Exception as e:
            print(f"❌ Error durante el análisis: {e}")
            return f"Ocurrió un error durante el análisis: {e}"
