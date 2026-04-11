from flask import Blueprint, render_template_string, request, redirect, url_for, flash, session

from extensions import db
from models import User
from utils import auth_required, role_required, FULL_ACCESS_ROLES
from ui import USERS_HTML, SETUP_HTML

bp = Blueprint("users", __name__)

_ADMIN_ONLY = {"admin"}


@bp.route("/setup", methods=["GET", "POST"])
def setup():
    """First-run only — create the initial admin account."""
    if User.query.first() is not None:
        return redirect(url_for("publishing.login"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        email    = request.form.get("email", "").strip()
        password = request.form.get("password", "")
        confirm  = request.form.get("confirm_password", "")

        if not username or not password:
            flash("Username and password are required.", "error")
            return render_template_string(SETUP_HTML)
        if password != confirm:
            flash("Passwords do not match.", "error")
            return render_template_string(SETUP_HTML)
        if len(password) < 8:
            flash("Password must be at least 8 characters.", "error")
            return render_template_string(SETUP_HTML)

        user = User(username=username, email=email, role="admin", is_active=True)
        user.set_password(password)
        db.session.add(user)
        db.session.commit()
        flash("Admin account created — please log in.", "success")
        return redirect(url_for("publishing.login"))

    return render_template_string(SETUP_HTML)


@bp.route("/users")
def users_list():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Only admins can manage users.", "error")
        return redirect(url_for("publishing.works_list"))

    users = User.query.order_by(User.created_at.asc()).all()
    return render_template_string(USERS_HTML, users=users)


@bp.route("/users/create", methods=["POST"])
def create_user():
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Only admins can create users.", "error")
        return redirect(url_for("publishing.works_list"))

    username = request.form.get("username", "").strip()
    email    = request.form.get("email", "").strip()
    password = request.form.get("password", "")
    role     = request.form.get("role", "ar").strip()

    if not username or not password:
        flash("Username and password are required.", "error")
        return redirect(url_for("users.users_list"))
    if len(password) < 8:
        flash("Password must be at least 8 characters.", "error")
        return redirect(url_for("users.users_list"))
    if role not in ("admin", "label_manager", "publishing_manager", "ar"):
        flash("Invalid role.", "error")
        return redirect(url_for("users.users_list"))
    if User.query.filter_by(username=username).first():
        flash(f"Username '{username}' is already taken.", "error")
        return redirect(url_for("users.users_list"))

    user = User(username=username, email=email, role=role, is_active=True)
    user.set_password(password)
    db.session.add(user)
    db.session.commit()
    flash(f"User '{username}' created.", "success")
    return redirect(url_for("users.users_list"))


@bp.route("/users/<int:user_id>/toggle", methods=["POST"])
def toggle_user(user_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Only admins can manage users.", "error")
        return redirect(url_for("publishing.works_list"))

    user = User.query.get_or_404(user_id)
    if user.id == session.get("user_id"):
        flash("You cannot deactivate your own account.", "error")
        return redirect(url_for("users.users_list"))

    user.is_active = not user.is_active
    db.session.commit()
    status = "activated" if user.is_active else "deactivated"
    flash(f"User '{user.username}' {status}.", "success")
    return redirect(url_for("users.users_list"))


@bp.route("/users/<int:user_id>/role", methods=["POST"])
def change_role(user_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Only admins can change roles.", "error")
        return redirect(url_for("publishing.works_list"))

    user = User.query.get_or_404(user_id)
    new_role = request.form.get("role", "").strip()
    if new_role not in ("admin", "label_manager", "publishing_manager", "ar"):
        flash("Invalid role.", "error")
        return redirect(url_for("users.users_list"))

    user.role = new_role
    db.session.commit()
    flash(f"Role updated for '{user.username}'.", "success")
    return redirect(url_for("users.users_list"))


@bp.route("/users/<int:user_id>/password", methods=["POST"])
def reset_password(user_id):
    if auth_required():
        return redirect(url_for("publishing.login"))
    if role_required(_ADMIN_ONLY):
        flash("Only admins can reset passwords.", "error")
        return redirect(url_for("publishing.works_list"))

    user = User.query.get_or_404(user_id)
    new_password = request.form.get("new_password", "")
    if len(new_password) < 8:
        flash("Password must be at least 8 characters.", "error")
        return redirect(url_for("users.users_list"))

    user.set_password(new_password)
    db.session.commit()
    flash(f"Password reset for '{user.username}'.", "success")
    return redirect(url_for("users.users_list"))
