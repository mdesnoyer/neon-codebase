
"""Forgot Password Email Template

The template function returns a tuple (subject, body).
"""

def template():
    return (
"Password Reset Request",
"""<html>
  <body>
    <p><a href="https://www.neon-lab.com">
    <img class="logo"
     style = "height: 43.25px; width: 104px;"
     src="https://s3.amazonaws.com/neon_website_assets/logo_777.png"
     alt="Neon">
    </a></p>
    <br>
    <p>Hi {first_name},</p>
    <p>
      Sorry to hear, you've forgotten your
      password. But, have no fear the link to
      reset it is right below. This link will
      be good for the next hour.
      <p>
        <a style="color: #f16122" href="{url}">
          Reset Your Password
        </a>
      </p>
    </p>
    <p>
      You have received this message because
      someone requested to have the password
      reset for this user. If this was not you,
      don't worry. Someone may have typed in
      your username by mistake.
    </p>
    <p>
      Nobody will be able to change your password
      without the token contained in the link above.
      When you have completed the password change,
      you can delete this email.
    </p>
    <p>Thanks,<br>
      The Neon Team
    </p>
    <p style="font-size:smaller">
       If the reset password link does not work
       please copy and paste the following address
       into your browser : {url}
    </p>
    <p>------------</p>
    <p class="footer" style="font-size: smaller">Neon Labs Inc.<br>
    70 South Park St. | San Francisco, CA 94107<br>
    (415) 355-4249<br>
    <a style="color: #f16122" href="https://www.neon-lab.com">neon-lab.com</a>
    </p>
    <p class="footer" style="font-size: smaller">This is an automated email.
    Please get in touch with us at
    <a href="mailto:ask@neon-lab.com">
      ask@neon-lab.com</a></p>
  </body>
</html>""")
