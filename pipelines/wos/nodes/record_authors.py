def split_wos_authors(au: str, af: str):
    au_names = au.split("; ")
    af_names = af.split("; ")

    assert len(au_names) == len(af_names), "AU and AF lengths do not match"

    for au_name, full_name in zip(au_names, af_names):
        name_parts = au_name.split(", ", 1)

        yield {
            "last_name": name_parts[0],
            "first_name": name_parts[1] if len(name_parts) > 1 else None,
            "full_name": full_name,
        }
