import json

import requests
from loguru import logger as log


def get_category_paths(categories, parent_path=[]):
    paths = []
    for category in categories:
        current_path = parent_path + [category["slug_parce"]]
        paths.append("/".join(current_path))
        if category["children"]:
            paths.extend(get_category_paths(category["children"], current_path))
    return paths


def formatted_products(products):
    products_formatted = list()

    for product in products:
        size_found = False
        categories = product.get('nav > ul > li > a', [])
        #categories.pop(0)
        #categories.pop()
        categories = [category.get('name') for category in categories]

        characteristics = list()
        formatted_variants = dict()

        traits = product.get('traits', [])
        for trait in traits:
            if trait.get('visible'):
                characteristics.append({
                    "name": trait.get('name'),
                    "value": trait.get('value')
                })

        images = product.get('media', {})
        images.pop("smallImageUrl"),
        images.pop("thumbUrl")

        variants = product.get('variants', [])
        if product.get('productCategory') != "sneakers":
            continue
        for variant in variants:
            display_options = variant.get('sizeChart', {}).get('displayOptions', [])
            for display_option in display_options:
                if display_option.get('type') == 'eu':
                    size_found = True
                    price = variant.get('market', {}).get(
                        'bidAskData', {}
                    ).get('lowestAsk')
                    size = display_option.get('size')
                    if price is not None:
                        formatted_variants[variant.get('sizeChart', {}).get('baseSize')] = {
                            "name": size,
                            "value": price,
                        }
                    else:
                        formatted_variants[variant.get('sizeChart', {}).get('baseSize')] = {
                            "name": size,
                            "value": "BID",
                        }
        if not size_found:
            continue
        product_price = product.get('market', {}).get('bidAskData', {}).get('lowestAsk')
        if product_price is None:
            product_price = "BID"
        data_product = {
            "url": f"https://stockx.com/{product.get('urlKey')}",
            "name": product.get('primaryTitle'),
            "slug": product.get('urlKey'),
            "price": product_price,
            "last_sale": product.get('market', {}).get('salesInformation', {}).get('lastSale'),
            "gender": product.get('gender'),
            "brand": product.get('brand'),
            "description": product.get('description'),
            "categories": categories,
            "characteristics": characteristics,
            "variants": formatted_variants,
            "images": images,

        }
        products_formatted.append(data_product)

    data = {
        "status": "0",
        "source": "stockX",
        "category": "sneakers",
        "products": products_formatted
    }
    return data


def post_products(products_formatted):
    with open('products.json', 'r') as f1:
        products = json.load(f1)
    products.append(products_formatted)
    print("Products count:", len(products))
    with open('products.json', 'w') as f:
        json.dump(products, f,indent=2)