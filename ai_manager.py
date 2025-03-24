import os
import json
import openai


class AIAnalysisManager:
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        openai.api_key = self.api_key

    def load_prompt(self, prompt_name):
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

            # Llamada a OpenAI
            response = openai.chat.completions.create(
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
