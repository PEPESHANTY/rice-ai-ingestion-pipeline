from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

model_name = "facebook/nllb-200-distilled-600M"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSeq2SeqLM.from_pretrained(model_name)

# Set source language
tokenizer.src_lang = "vie_Latn"

def translate_vi_en(text: str):
    inputs = tokenizer(text, return_tensors="pt", padding=True)
    #  target lang forced_bos_token_id for English = 65001
    generated_tokens = model.generate(
        **inputs,
        forced_bos_token_id=tokenizer.convert_tokens_to_ids(">>eng_Latn<<")
    )
    return tokenizer.batch_decode(generated_tokens, skip_special_tokens=True)[0]

# Test it
vi_text = "Phân bón hữu cơ giúp cải thiện độ phì nhiêu của đất."
en_translation = translate_vi_en(vi_text)
# print("✅ Translated:", en_translation)
