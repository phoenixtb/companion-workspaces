-- Seed data for local runs and tests: 5 suppliers, 16 products.

insert into suppliers (id, code, name) values
  (uuidv7(), 'SUP-001', 'Banyan Agro Supplies'),
  (uuidv7(), 'SUP-002', 'Heritage Foods & Grains'),
  (uuidv7(), 'SUP-003', 'Valley Fresh Producers'),
  (uuidv7(), 'SUP-004', 'Shri Krishna Consumer Goods'),
  (uuidv7(), 'SUP-005', 'Himalayan Artisan Products');

insert into products (id, sku, name, description, category, price_paise, supplier_id, created_at, version) values
  (uuidv7(), 'SHW-1001', 'Durum Wheat Penne 500 g', 'Made from hard durum wheat semolina; cooks to al dente texture.', 'PASTA_AND_GRAINS', 16500, (select id from suppliers where code = 'SUP-001'), timestamptz '2026-09-01 09:00:00+05:30', 0),
  (uuidv7(), 'SHW-1002', 'Brown Rice 1 kg', 'Whole-grain brown rice with the bran layer intact; cooks in about 35 minutes.', 'PASTA_AND_GRAINS', 8500, (select id from suppliers where code = 'SUP-002'), timestamptz '2026-09-01 09:00:00+05:30', 0),
  (uuidv7(), 'SHW-1003', 'Gluten-Free Millet Pasta 250 g', 'Made from millet flour and lentil flour; certified gluten-free and naturally vegan.', 'PASTA_AND_GRAINS', 19500, (select id from suppliers where code = 'SUP-003'), timestamptz '2026-09-01 09:00:00+05:30', 0),
  (uuidv7(), 'SHW-1004', 'Gluten-Free Quinoa Pasta 300 g', 'Blended with quinoa and rice flour for complete protein; certified gluten-free and vegan.', 'PASTA_AND_GRAINS', 25000, (select id from suppliers where code = 'SUP-004'), timestamptz '2026-09-01 09:00:00+05:30', 0),
  (uuidv7(), 'SHW-1005', 'Whole Wheat Flour 2 kg', 'Stone-ground whole wheat with the germ intact; suitable for rotis and baking.', 'PASTA_AND_GRAINS', 6500, (select id from suppliers where code = 'SUP-001'), timestamptz '2026-09-01 09:00:00+05:30', 0),
  (uuidv7(), 'SHW-1006', 'Full Cream Milk Powder 500 g', 'Spray-dried whole milk powder; dissolves in warm water for tea, coffee and cooking.', 'DAIRY', 35000, (select id from suppliers where code = 'SUP-002'), timestamptz '2026-09-01 09:00:00+05:30', 0),
  (uuidv7(), 'SHW-1007', 'Organic Ghee 500 ml', 'Clarified cow-milk butter from certified organic farms; shelf-stable for nine months unopened.', 'DAIRY', 55000, (select id from suppliers where code = 'SUP-003'), timestamptz '2026-09-01 09:00:00+05:30', 0),
  (uuidv7(), 'SHW-1008', 'Green Tea Bags 25 units', 'Green tea leaves in pyramid tea bags; steep for two to three minutes at 80 °C.', 'BEVERAGES', 12500, (select id from suppliers where code = 'SUP-004'), timestamptz '2026-09-01 09:00:00+05:30', 0),
  (uuidv7(), 'SHW-1009', 'Mixed Nuts & Raisins 250 g', 'Almonds, cashews and raisins, unsalted; packed in a resealable pouch.', 'SNACKS', 22500, (select id from suppliers where code = 'SUP-005'), timestamptz '2026-09-01 09:00:00+05:30', 0),
  (uuidv7(), 'SHW-1010', 'Honey 500 ml', 'Unprocessed multiflora honey from Himalayan apiaries; may crystallise in cold weather.', 'BREAKFAST', 28000, (select id from suppliers where code = 'SUP-001'), timestamptz '2026-09-01 09:00:00+05:30', 0),
  (uuidv7(), 'SHW-1011', 'Instant Oatmeal 400 g', 'Rolled oats with added vitamins and minerals; quick-cook breakfast option.', 'BREAKFAST', 8500, (select id from suppliers where code = 'SUP-002'), timestamptz '2026-09-01 09:00:00+05:30', 0),
  (uuidv7(), 'SHW-1012', 'Natural Shampoo 200 ml', 'Made from neem oil and herbal extracts; sulfate-free formula.', 'PERSONAL_CARE', 15000, (select id from suppliers where code = 'SUP-003'), timestamptz '2026-09-01 09:00:00+05:30', 0),
  (uuidv7(), 'SHW-1013', 'Mixer Grinder 1200W', 'Heavy-duty motor with stainless steel jars; suitable for grinding spices and wet grinding.', 'SMALL_APPLIANCES', 299900, (select id from suppliers where code = 'SUP-004'), timestamptz '2026-09-01 09:00:00+05:30', 0),
  (uuidv7(), 'SHW-1014', 'Electric Kettle 1.5L', 'Auto shut-off feature; boils water in 3–5 minutes with cool-touch handle.', 'SMALL_APPLIANCES', 189500, (select id from suppliers where code = 'SUP-005'), timestamptz '2026-09-01 09:00:00+05:30', 0),
  (uuidv7(), 'SHW-1015', 'Basmati Rice 2 kg', 'Long-grain white basmati, aged twelve months; grains stay separate after cooking.', 'PASTA_AND_GRAINS', 28000, (select id from suppliers where code = 'SUP-001'), timestamptz '2026-09-01 09:00:00+05:30', 0),
  (uuidv7(), 'SHW-1016', 'Roasted Chickpea Snack 150 g', 'Whole roasted chickpeas with mild spices; high in protein and fiber.', 'SNACKS', 9500, (select id from suppliers where code = 'SUP-002'), timestamptz '2026-09-01 09:00:00+05:30', 0);

insert into product_dietary_tags (product_id, tag) values
  ((select id from products where sku = 'SHW-1002'), 'vegetarian'),
  ((select id from products where sku = 'SHW-1003'), 'gluten-free'),
  ((select id from products where sku = 'SHW-1003'), 'vegan'),
  ((select id from products where sku = 'SHW-1004'), 'gluten-free'),
  ((select id from products where sku = 'SHW-1004'), 'vegan'),
  ((select id from products where sku = 'SHW-1004'), 'high-protein'),
  ((select id from products where sku = 'SHW-1005'), 'vegetarian'),
  ((select id from products where sku = 'SHW-1006'), 'vegetarian'),
  ((select id from products where sku = 'SHW-1007'), 'vegetarian'),
  ((select id from products where sku = 'SHW-1007'), 'organic'),
  ((select id from products where sku = 'SHW-1008'), 'vegan'),
  ((select id from products where sku = 'SHW-1009'), 'vegetarian'),
  ((select id from products where sku = 'SHW-1010'), 'vegetarian'),
  ((select id from products where sku = 'SHW-1011'), 'vegetarian'),
  ((select id from products where sku = 'SHW-1016'), 'vegan'),
  ((select id from products where sku = 'SHW-1016'), 'high-protein');
