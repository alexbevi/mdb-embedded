#!/usr/bin/env python3
"""
04_crewai_agent_tool.py -- CrewAI agents query smongo over the wire protocol.

Starts smongo's embedded wire server, then gives a CrewAI agent a standard
PyMongo-based tool to query the "company database". CrewAI and PyMongo have
no idea they're talking to an in-process engine -- smongo is invisible.

Requirements:
    pip install crewai pymongo smongo

Run:
    python examples/ai_examples/04_crewai_agent_tool.py
"""

import os
import shutil
import sys
import tempfile
import time

from smongo import MongoClient as SmongoClient
from smongo import WireServer

PORT = 27021


def main() -> None:
    try:
        from crewai import Agent, Crew, Process, Task
        from langchain_core.tools import tool

        HAS_CREWAI = True
    except ImportError:
        HAS_CREWAI = False

    db_path = tempfile.mkdtemp(prefix="smongo_crew_wire_")

    # ── 1. Seed data via native smongo ─────────────────────────
    print("── CrewAI + smongo (wire protocol) ──\n")
    print("1. Seeding company database via native smongo client...")

    native = SmongoClient(f"local://{db_path}")
    employees = native["company"]["employees"]
    projects = native["company"]["projects"]

    employees.insert_many(
        [
            {
                "name": "Alice",
                "role": "Senior Engineer",
                "skills": ["Python", "Rust", "MongoDB"],
                "team": "platform",
            },
            {
                "name": "Bob",
                "role": "Product Manager",
                "skills": ["Agile", "Scrum", "SQL"],
                "team": "product",
            },
            {
                "name": "Charlie",
                "role": "Data Scientist",
                "skills": ["Python", "ML", "PyTorch"],
                "team": "ai",
            },
            {
                "name": "Diana",
                "role": "DevOps Engineer",
                "skills": ["Docker", "Kubernetes", "AWS"],
                "team": "platform",
            },
            {
                "name": "Eve",
                "role": "ML Engineer",
                "skills": ["Python", "TensorFlow", "CUDA"],
                "team": "ai",
            },
            {
                "name": "Frank",
                "role": "Backend Engineer",
                "skills": ["Go", "PostgreSQL", "gRPC"],
                "team": "platform",
            },
        ]
    )

    projects.insert_many(
        [
            {
                "name": "RAG Pipeline",
                "required_skills": ["Python", "ML", "MongoDB"],
                "status": "planning",
            },
            {
                "name": "API Gateway",
                "required_skills": ["Go", "Docker", "gRPC"],
                "status": "active",
            },
            {
                "name": "Data Warehouse",
                "required_skills": ["Python", "SQL", "AWS"],
                "status": "planning",
            },
        ]
    )

    print(
        f"   {employees.count_documents({})} employees, {projects.count_documents({})} projects seeded."
    )

    # ── 2. Start wire server ───────────────────────────────────
    print(f"\n2. Starting wire protocol server on port {PORT}...")

    with WireServer(db_path, port=PORT, local_client=native.get_local_client()) as _srv:
        time.sleep(0.3)

        # ── 3. Connect with STANDARD PyMongo ───────────────────
        from pymongo import MongoClient as PyMongoClient

        client = PyMongoClient(
            f"mongodb://localhost:{PORT}",
            serverSelectionTimeoutMS=5000,
            directConnection=True,
        )
        emp_coll = client["company"]["employees"]
        proj_coll = client["company"]["projects"]

        print(f"   PyMongo sees {emp_coll.count_documents({})} employees via wire protocol.\n")

        # ── 4. Define tools using standard PyMongo ─────────────
        # These tools use plain pymongo -- zero smongo imports!

        if HAS_CREWAI:
            from langchain_core.tools import tool

            @tool("Search Employees by Skill")
            def search_by_skill(skill: str) -> str:
                """Search the company database for employees with a specific skill."""
                results = list(
                    emp_coll.find(
                        {"skills": {"$regex": skill, "$options": "i"}},
                        {"_id": 0, "name": 1, "role": 1, "team": 1, "skills": 1},
                    )
                )
                if not results:
                    return f"No employees found with skill: {skill}"
                lines = [f"- {r['name']} ({r['role']}, team: {r['team']})" for r in results]
                return f"Found {len(results)} employees with '{skill}':\n" + "\n".join(lines)

            @tool("Find Projects Needing Staff")
            def find_projects(status: str) -> str:
                """Find projects by status (planning/active) and their required skills."""
                results = list(
                    proj_coll.find(
                        {"status": status}, {"_id": 0, "name": 1, "required_skills": 1, "status": 1}
                    )
                )
                if not results:
                    return f"No projects with status: {status}"
                lines = [
                    f"- {r['name']} (needs: {', '.join(r['required_skills'])})" for r in results
                ]
                return f"Found {len(results)} {status} projects:\n" + "\n".join(lines)

        # ── 5. Run the agent (or simulate) ─────────────────────
        has_api_key = bool(os.environ.get("OPENAI_API_KEY"))

        if HAS_CREWAI and has_api_key:
            print("3. Running CrewAI agent with live LLM...\n")

            resource_manager = Agent(
                role="Resource Manager",
                goal="Match employees to projects based on their skills and project requirements.",
                backstory="You are an expert at assembling technical teams. You query the company database to find the best people for each project.",
                verbose=True,
                allow_delegation=False,
                tools=[search_by_skill, find_projects],
            )

            task = Task(
                description="Find all planning-phase projects, then for each project find employees whose skills match. Recommend a team for the RAG Pipeline project.",
                expected_output="A recommended team for the RAG Pipeline project with justification.",
                agent=resource_manager,
            )

            crew = Crew(agents=[resource_manager], tasks=[task], process=Process.sequential)
            result = crew.kickoff()
            print(f"\n── Agent Output ──\n{result}")

        else:
            print("3. Simulating agent tool calls (no OPENAI_API_KEY / crewai)...\n")
            print("   The agent would call these tools over standard PyMongo:\n")

            # Simulate: agent looks for planning projects
            planning = list(proj_coll.find({"status": "planning"}, {"_id": 0}))
            print("   Tool: Find Projects Needing Staff('planning')")
            for p in planning:
                print(f"     -> {p['name']} (needs: {', '.join(p['required_skills'])})")

            # Simulate: agent searches for Python + ML engineers
            print("\n   Tool: Search Employees by Skill('Python')")
            python_devs = list(
                emp_coll.find(
                    {"skills": {"$regex": "Python", "$options": "i"}},
                    {"_id": 0, "name": 1, "role": 1, "skills": 1},
                )
            )
            for e in python_devs:
                skills_str = ", ".join(e.get("skills", []))
                print(f"     -> {e['name']} ({e['role']}): {skills_str}")

            print("\n   Tool: Search Employees by Skill('MongoDB')")
            mongo_devs = list(
                emp_coll.find(
                    {"skills": {"$regex": "MongoDB", "$options": "i"}},
                    {"_id": 0, "name": 1, "role": 1, "skills": 1},
                )
            )
            for e in mongo_devs:
                skills_str = ", ".join(e.get("skills", []))
                print(f"     -> {e['name']} ({e['role']}): {skills_str}")

            print("\n   Agent recommendation (simulated):")
            print(
                "     RAG Pipeline team: Alice (Python + MongoDB), Charlie (Python + ML), Eve (Python + TensorFlow)"
            )
            print("\n   All queries went through standard PyMongo over the wire protocol.")
            print("   CrewAI and PyMongo had no idea smongo was the engine.\n")

        client.close()

    native.close()
    shutil.rmtree(db_path, ignore_errors=True)
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
