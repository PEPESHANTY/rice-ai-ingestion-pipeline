# translation_utils.py
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

model_name = "facebook/nllb-200-distilled-600M"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSeq2SeqLM.from_pretrained(model_name)

tokenizer.src_lang = "vie_Latn"

def translate_vi_en(text: str) -> str:
    inputs = tokenizer(text, return_tensors="pt", padding=True)
    generated_tokens = model.generate(
        **inputs,
        forced_bos_token_id=tokenizer.convert_tokens_to_ids(">>eng_Latn<<")
    )
    return tokenizer.batch_decode(generated_tokens, skip_special_tokens=True)[0]
