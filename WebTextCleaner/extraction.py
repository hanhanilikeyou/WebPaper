import justext

def extract_main_text(html_text):
    paragraphs = justext.justext(html_text.encode("utf-8"), justext.get_stoplist("English"))
    return [para.text for para in paragraphs if not para.is_boilerplate]
