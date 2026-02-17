from eralchemy2 import render_er

# Connection string for your local MySQL (no password)
# Format: mysql+mysqlconnector://user@host/database
db_url = 'mysql+mysqlconnector://root@127.0.0.1/CryptoBot_Step2'

try:
    # Generate the diagram as a PNG file
    render_er(db_url, 'Step2_UML_Diagram.png')
    print("Success! ER-Diagram saved as 'Step2_ER_Diagram.png'.")
except Exception as e:
    print(f"Error: {e}")