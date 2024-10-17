# Roles

This module is designed to manage roles, vetting processes, and member activities within a Discord server.

## Features

- Custom role management and assignment
- Automated vetting system with approval/rejection voting
- Servant role directory viewing
- Penitentiary system for temporary member restrictions
- Activity-based automatic role updates
- Comprehensive logging of role changes and system actions

## Usage

### Custom Roles

- `/roles custom configure`: Add or remove custom roles
- Use the **Custom Roles** context menu on a user to assign custom roles

### Vetting

- Vetting threads are automatically created in the designated forum
- Use the **Approve** and **Reject** buttons in vetting threads to vote

### Servant Directory

- `/roles servant view`: Display a paginated list of all servant roles and their members

### Penitentiary

- `/roles penitentiary incarcerate`: Incarcerate a member for a specified duration
- `/roles penitentiary release`: Manually release an incarcerated member

## Configuration

The module uses a `Config` class for its configuration. Key settings include:

- `VETTING_FORUM_ID`: ID of the forum channel for vetting threads
- `VETTING_ROLE_IDS`: List of role IDs allowed to participate in vetting
- `ELECTORAL_ROLE_ID`: ID of the role granted upon successful vetting
- `APPROVED_ROLE_ID`: ID of the approved member role
- `TEMPORARY_ROLE_ID`: ID of the temporary member role
- `INCARCERATED_ROLE_ID`: ID of the role assigned to incarcerated members
- `AUTHORIZED_CUSTOM_ROLE_IDS`: List of role IDs allowed to manage custom roles
- `AUTHORIZED_PENITENTIARY_ROLE_IDS`: List of role IDs allowed to use penitentiary commands
- `REQUIRED_APPROVALS`: Number of approvals required for successful vetting
- `REQUIRED_REJECTIONS`: Number of rejections required to reject a vetting request
- `REJECTION_WINDOW_DAYS`: Number of days after approval during which a member can be rejected
- `LOG_FORUM_ID`: ID of the forum channel for logging module actions
- `LOG_POST_ID`: ID of the specific post in the log forum for module logs
- `GUILD_ID`: ID of the Discord server (guild) where the module operates

Adjust these values in the `Config` class to customize the module's behavior for your server.
