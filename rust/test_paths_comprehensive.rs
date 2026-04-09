// Comprehensive test to verify Rust paths matches Python behavior
use bson::{doc, Bson, Document};

fn main() {
    // Import from smongo-engine
    use smongo_engine::paths::{get_value, field_exists, set_value, unset_value};

    println!("=== Testing get_value ===");

    // Test 1: Simple field
    let doc = doc! { "name": "Alice" };
    assert_eq!(get_value(&doc, "name"), Some(&Bson::String("Alice".to_string())));
    assert_eq!(get_value(&doc, "missing"), None);
    println!("✓ Simple field access");

    // Test 2: Null value - should return Some(&Bson::Null), not None
    let doc = doc! { "value": Bson::Null };
    assert_eq!(get_value(&doc, "value"), Some(&Bson::Null));
    println!("✓ Null value returns Some(&Null)");

    // Test 3: Traversing through null should return None
    let doc = doc! { "value": Bson::Null };
    assert_eq!(get_value(&doc, "value.nested"), None);
    println!("✓ Traversing through null returns None");

    // Test 4: Nested document
    let doc = doc! { "user": { "name": "Bob" } };
    assert_eq!(get_value(&doc, "user.name"), Some(&Bson::String("Bob".to_string())));
    assert_eq!(get_value(&doc, "user.age"), None);
    println!("✓ Nested document");

    // Test 5: Array access
    let doc = doc! { "items": [1, 2, 3] };
    assert_eq!(get_value(&doc, "items.0"), Some(&Bson::Int32(1)));
    assert_eq!(get_value(&doc, "items.5"), None);
    println!("✓ Array access");

    // Test 6: Mixed nested with arrays
    let doc = doc! { "data": { "items": [1, 2, 3] } };
    assert_eq!(get_value(&doc, "data.items.1"), Some(&Bson::Int32(2)));
    println!("✓ Mixed nesting");

    // Test 7: Empty doc
    let doc = Document::new();
    assert_eq!(get_value(&doc, "anything"), None);
    println!("✓ Empty document");

    // Test 8: Traversing through null in nested path
    let doc = doc! { "a": { "b": Bson::Null } };
    assert_eq!(get_value(&doc, "a.b"), Some(&Bson::Null));
    assert_eq!(get_value(&doc, "a.b.c"), None);  // Can't traverse through null
    println!("✓ Nested null traversal");

    // Test 9: Trying to traverse through a non-document/array
    let doc = doc! { "value": 42 };
    assert_eq!(get_value(&doc, "value.nested"), None);
    println!("✓ Can't traverse through scalar");

    println!("\n=== Testing field_exists ===");

    // Test: field_exists with null value should return true
    let doc = doc! { "value": Bson::Null };
    assert_eq!(field_exists(&doc, "value"), true);
    assert_eq!(field_exists(&doc, "missing"), false);
    println!("✓ field_exists distinguishes null from missing");

    println!("\n=== Testing set_value ===");

    let mut doc = Document::new();
    set_value(&mut doc, "a.b.c", Bson::Int32(42)).unwrap();
    assert_eq!(get_value(&doc, "a.b.c"), Some(&Bson::Int32(42)));
    println!("✓ set_value creates nested documents");

    println!("\n=== Testing unset_value ===");

    let mut doc = doc! { "user": { "name": "Bob", "age": 30 } };
    unset_value(&mut doc, "user.name").unwrap();
    assert_eq!(field_exists(&doc, "user.name"), false);
    assert_eq!(field_exists(&doc, "user.age"), true);
    println!("✓ unset_value removes nested field");

    println!("\n✅ All comprehensive tests passed!");
}
