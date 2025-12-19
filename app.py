import streamlit as st
import streamlit.components.v1 as components

def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == st.secrets["password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct.
        return True

# Create a secrets.toml file locally with your password
# and Power BI report URL.
# [password]
# password = "your_password"
# [power_bi]
# report_url = "your_report_url"

# In Streamlit Cloud, add these secrets to your app's settings.
if "password" not in st.secrets:
    st.error("Password not found in secrets. Please add a password to your secrets.toml file.")
else:
    if check_password():
        st.title("Power BI Report")

        if "report_url" not in st.secrets.get("power_bi", {}):
            st.warning("Power BI report URL not found in secrets. Please add the report URL to your secrets.toml file.")
        else:
            power_bi_url = st.secrets["power_bi"]["report_url"]
            components.iframe(power_bi_url, height=600, scrolling=True)
